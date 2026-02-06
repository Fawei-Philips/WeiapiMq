# main.py
# Python 3.10.10
# !pip install "opencv-python<4.10" pika requests segmentation-models-pytorch albumentations matplotlib torch

import os, glob, cv2, numpy as np, torch
import matplotlib.pyplot as plt
import tempfile
from urllib.request import urlretrieve
import traceback

try:
    import requests
except ImportError:
    requests = None
from albumentations.pytorch import ToTensorV2
import segmentation_models_pytorch as smp

# 导入独立的 RabbitMQ 模块
from rabbitmq_consumer import start_rabbitmq_consumer, RabbitMQConfig

# ===================== 全局配置 =====================
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
IMG_H, IMG_W = 256, 256
base_dir = os.getcwd()
results_dir = os.path.join(base_dir, 'results')
os.makedirs(results_dir, exist_ok=True)

print("=" * 50)
print("初始化环境...")
print(f"结果保存路径: {results_dir}")
print(f"使用设备: {DEVICE}")
print("=" * 50)


# ===================== 模型加载逻辑 =====================
def find_model_weights():
    candidates = [
        'best_stroke_model_with_normals.pth',
        os.path.join(base_dir, 'best_stroke_model_with_normals.pth'),
        os.path.join(base_dir, 'working', 'best_stroke_model_with_normals.pth'),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    search_roots = [base_dir, os.path.dirname(base_dir), os.path.join(base_dir, 'Stroke_segmentation_UNet')]
    for root in search_roots:
        for p in glob.glob(os.path.join(root, '**', 'best_stroke_model_with_normals.pth'), recursive=True):
            return p
    return None


print("定位模型权重...")
MODEL_WEIGHTS = find_model_weights()
if MODEL_WEIGHTS is None:
    raise FileNotFoundError("找不到 'best_stroke_model_with_normals.pth'，请手动指定路径")
print(f"使用模型权重: {MODEL_WEIGHTS}")


def build_model_offline():
    return smp.Unet(
        encoder_name='efficientnet-b4',
        encoder_weights=None,
        in_channels=3,
        classes=1,
        activation=None,
    )


# 加载模型
model = build_model_offline().to(DEVICE)
state = torch.load(MODEL_WEIGHTS, map_location=DEVICE)
model.load_state_dict(state)
model.eval()
print("模型加载完成！")


# ===================== 图片处理核心逻辑 =====================
def get_transforms(is_training=True):
    import albumentations as A
    return A.Compose([
        A.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ToTensorV2(),
    ])


tform = get_transforms(False)


def download_image_from_url(url, save_dir=None):
    """下载网络图片到临时文件（供 RabbitMQ 回调调用）"""
    if save_dir is None:
        save_dir = tempfile.gettempdir()
    try:
        fname = f"temp_{hash(url)}_{os.path.basename(url)}"
        save_path = os.path.join(save_dir, fname)
        urlretrieve(url, save_path)
        print(f"图片下载完成: {save_path}")
        return save_path
    except Exception as e:
        print(f"下载图片失败 {url}: {e}")
        return None


def post_outputs(url, overlay_path, prob_path, payload=None, timeout=15):
    """POST 结果到服务器（保留原有逻辑）"""
    if not url or requests is None:
        return False
    data = payload or {}
    try:
        with open(overlay_path, 'rb') as f1, open(prob_path, 'rb') as f2:
            files = {
                'overlay': (os.path.basename(overlay_path), f1, 'image/png'),
                'prob': (os.path.basename(prob_path), f2, 'image/png'),
            }
            resp = requests.post(url, data=data, files=files, timeout=timeout)
        ok = 200 <= resp.status_code < 300
        print(f"POST 结果: {url} -> {resp.status_code}")
        if not ok:
            print(f"POST 失败详情: {str(resp.text)[:500]}")
        return ok
    except Exception as e:
        print(f"POST 请求失败: {e}")
        return False


def process_image(img_path):
    """处理单张图片（核心函数，供 RabbitMQ 回调调用）"""
    try:
        # 1. 读取图片
        bgr = cv2.imread(img_path)
        if bgr is None:
            print(f"无法读取图片: {img_path}")
            return False

        # 2. 预处理
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
        rgb_r = cv2.resize(rgb, (IMG_W, IMG_H), interpolation=cv2.INTER_LINEAR)

        # 3. 模型推理
        with torch.no_grad():
            x = tform(image=rgb_r, mask=np.zeros((IMG_H, IMG_W), np.float32))['image'].unsqueeze(0).to(DEVICE)
            logits = model(x)[0, 0].cpu().numpy()
            prob = 1 / (1 + np.exp(-logits))  # sigmoid
            pred_bin = (prob > 0.5).astype(np.uint8) * 255

        # 4. 生成 overlay
        overlay = rgb_r.copy()
        overlay[pred_bin > 0] = (255, 64, 64)

        # 5. 保存结果
        stem = os.path.splitext(os.path.basename(img_path))[0]
        out_overlay = os.path.join(results_dir, f"{stem}_overlay.png")
        out_prob = os.path.join(results_dir, f"{stem}_prob.png")
        cv2.imwrite(out_overlay, cv2.cvtColor(overlay, cv2.COLOR_RGB2BGR))
        prob_u8 = (np.clip(prob, 0, 1) * 255).astype(np.uint8)
        cv2.imwrite(out_prob, prob_u8)

        print(f"✅ 处理完成: {img_path}")
        print(f"   - Overlay: {out_overlay}")
        print(f"   - Prob: {out_prob}")

        # 可选：POST 结果
        post_url = os.getenv('POST_URL')
        if post_url:
            post_outputs(post_url, out_overlay, out_prob, payload={'image': stem, 'status': 'done'})

        return True
    except Exception as e:
        print(f"❌ 处理图片失败 {img_path}: {e}")
        traceback.print_exc()
        return False


# ===================== 程序入口 =====================
if __name__ == "__main__":
    # 自定义 RabbitMQ 配置（可选）
    custom_config = RabbitMQConfig()
    # custom_config.HOST = '192.168.1.100'  # 如需修改配置，直接改这里
    # custom_config.ROUTING_KEY = 'image.#'

    # 启动 RabbitMQ 消费者（传入图片处理/下载函数）
    start_rabbitmq_consumer(
        process_image_func=process_image,
        download_image_func=download_image_from_url,
        config=custom_config
    )