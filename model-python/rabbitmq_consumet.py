# rabbitmq_consumer.py
import pika
import json
import traceback
import time


# ===================== RabbitMQ 基础配置（可独立修改）=====================
class RabbitMQConfig:
    """RabbitMQ 配置类，集中管理配置项"""
    HOST = '127.0.0.1'
    PORT = 5672
    EXCHANGE = 'doraemon_topic'
    ROUTING_KEY = "image.url"  # 匹配发送端的 routing key，也可用通配符如 "image.#"
    QUEUE_NAME = 'image_process_queue'  # 固定队列名
    DURABLE_QUEUE = True  # 队列持久化
    AUTO_ACK = False  # 手动确认消息
    HEARTBEAT = 600  # 心跳超时（秒）
    MAX_RETRY = 3  # 消息最大重试次数


# ===================== RabbitMQ 消费者核心逻辑 =====================
def rabbitmq_callback(
        ch, method, properties, body,
        process_image_func,  # 主文件传入的图片处理函数
        download_image_func,  # 主文件传入的图片下载函数
        config=RabbitMQConfig()
):
    """
    RabbitMQ 消息回调函数（抽离到独立文件，通过参数接收主文件的处理逻辑）
    :param process_image_func: 主文件的图片处理函数
    :param download_image_func: 主文件的图片下载函数
    """
    try:
        # 1. 解析消息
        received_msg = body.decode('utf-8')
        print(f"\n📩 收到新消息: {received_msg}")

        # 2. 兼容纯URL/JSON格式消息
        image_url = None
        try:
            msg_data = json.loads(received_msg)
            image_url = msg_data.get('url') or msg_data.get('path')
        except json.JSONDecodeError:
            image_url = received_msg

        if not image_url:
            print("❌ 消息中未找到图片URL/路径")
            if not config.AUTO_ACK:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 3. 处理图片地址（网络URL/本地路径）
        img_path = None
        if image_url.startswith(('http://', 'https://')):
            img_path = download_image_func(image_url)  # 调用主文件的下载函数
        elif image_url and image_url.strip() and os.path.isfile(image_url):
            img_path = image_url
        else:
            print(f"❌ 无效的图片地址: {image_url}")
            if not config.AUTO_ACK:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 4. 执行图片处理（调用主文件的处理函数）
        temp_file_flag = False  # 标记是否是临时下载文件
        if img_path:
            temp_file_flag = image_url.startswith(('http://', 'https://'))
            process_success = process_image_func(img_path)  # 调用主文件的处理函数

            # 5. 手动确认/拒绝消息
            if not config.AUTO_ACK:
                if process_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print("✅ 消息已确认（处理成功）")
                else:
                    # 重试逻辑
                    retry_count = 1 if method.redelivered else 0
                    if retry_count < config.MAX_RETRY:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                        print(f"⚠️ 消息处理失败，重新入队（重试次数: {retry_count + 1}）")
                    else:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        print(f"❌ 消息重试{config.MAX_RETRY}次失败，丢弃: {image_url}")

        # 6. 清理临时文件
        if img_path and temp_file_flag and os.path.isfile(img_path):
            os.remove(img_path)
            print(f"🗑️ 清理临时文件: {img_path}")

    except Exception as e:
        print(f"\n❌ 回调函数执行失败: {e}")
        traceback.print_exc()
        # 异常时直接拒绝消息，避免死循环
        if not config.AUTO_ACK:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        pass


def start_rabbitmq_consumer(process_image_func, download_image_func, config=RabbitMQConfig()):
    """
    启动 RabbitMQ 持续消费者（对外暴露的核心函数）
    :param process_image_func: 主文件的图片处理函数
    :param download_image_func: 主文件的图片下载函数
    :param config: RabbitMQ 配置类实例
    """
    print("\n" + "=" * 50)
    print("启动 RabbitMQ 消费者（持续监听）...")
    print(f"Exchange: {config.EXCHANGE}, Queue: {config.QUEUE_NAME}")
    print("=" * 50)

    # 重连循环：连接断开后自动重试
    while True:
        try:
            # 1. 创建连接
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=config.HOST,
                    port=config.PORT,
                    heartbeat=config.HEARTBEAT,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()

            # 2. 声明交换机和队列
            channel.exchange_declare(
                exchange=config.EXCHANGE,
                exchange_type='topic',
                durable=True
            )
            channel.queue_declare(
                queue=config.QUEUE_NAME,
                durable=config.DURABLE_QUEUE,
                exclusive=False,
                auto_delete=False
            )
            channel.queue_bind(
                exchange=config.EXCHANGE,
                queue=config.QUEUE_NAME,
                routing_key=config.ROUTING_KEY
            )

            # 3. 设置 QoS（每次只处理1条消息）
            channel.basic_qos(prefetch_count=1)

            # 4. 注册回调函数（绑定主文件的处理函数）
            def callback_wrapper(ch, method, properties, body):
                """包装器：解决回调函数参数传递问题"""
                rabbitmq_callback(
                    ch, method, properties, body,
                    process_image_func=process_image_func,
                    download_image_func=download_image_func,
                    config=config
                )

            # 5. 启动消费（阻塞式运行）
            channel.basic_consume(
                queue=config.QUEUE_NAME,
                on_message_callback=callback_wrapper,
                auto_ack=config.AUTO_ACK
            )

            print(f"✅ 消费者已启动，等待消息...（按 Ctrl+C 停止）")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("❌ RabbitMQ 连接失败，5秒后重试...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n🛑 用户手动停止，关闭消费者...")
            if 'connection' in locals() and connection.is_open:
                connection.close()
            break
        except Exception as e:
            print(f"❌ 消费者异常: {e}")
            traceback.print_exc()
            time.sleep(5)


# 补充必要的导入（避免主文件未导入）
import os
import tempfile
from urllib.request import urlretrieve