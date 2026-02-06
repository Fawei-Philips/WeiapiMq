### 🚀 Ubuntu 虚拟机安装 RabbitMQ 完整指南
RabbitMQ 依赖 Erlang 运行时，我们按「环境准备 → 安装配置 → 验证测试」的流程来操作，适配你的 Ubuntu 22.04 LTS 环境：

---

## 🔹 第一步：安装依赖与配置官方源
### 1. 更新系统包
```bash
sudo apt update && sudo apt upgrade -y
```

### 2. 安装必备工具
```bash
sudo apt install -y curl gnupg apt-transport-https
```

### 🔍 错误原因
这个报错是因为 **Erlang Solutions 的源在国内访问不稳定**，导致 `curl` 请求超时（504 错误），无法获取有效的 OpenPGP 公钥，进而无法验证包的合法性。

---

### 🛠️ 解决方案：换用 RabbitMQ 官方维护的 Erlang 源（更稳定、兼容更好）
我们直接用 RabbitMQ 官方提供的 Erlang 源来安装，避免依赖访问不稳定的 Erlang Solutions 源。

#### 步骤 1：清理之前的错误文件
```bash
# 删除无效的公钥文件
sudo rm /usr/share/keyrings/erlang-solutions.gpg
# 删除无效的源文件
sudo rm /etc/apt/sources.list.d/erlang-solutions.list
```

#### 步骤 2：添加 RabbitMQ 官方的 Erlang 源
```bash
# 添加 RabbitMQ 官方 Erlang 源的公钥
curl -fsSL https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc | sudo gpg --dearmor -o /usr/share/keyrings/rabbitmq-archive-keyring.gpg

sudo gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys F77F1EDA57EBB1CC
sudo gpg --export F77F1EDA57EBB1CC | sudo tee /usr/share/keyrings/rabbitmq-erlang.gpg > /dev/null

# 添加 Erlang 源（由 RabbitMQ 官方维护，兼容性更好）
echo "deb [signed-by=/usr/share/keyrings/rabbitmq-erlang.gpg] https://ppa.launchpadcontent.net/rabbitmq/rabbitmq-erlang/ubuntu jammy main" | sudo tee /etc/apt/sources.list.d/rabbitmq-erlang.list
```

#### 步骤 3：更新包缓存并安装 Erlang
```bash
sudo apt update
# 安装适配 RabbitMQ 的 Erlang 版本
sudo apt install -y erlang-base erlang-crypto erlang-eldap erlang-inets erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key erlang-runtime-tools erlang-snmp erlang-ssl erlang-syntax-tools erlang-tftp erlang-tools erlang-xmerl
```

#### 步骤 4：继续安装 RabbitMQ
Erlang 安装完成后，回到之前的 RabbitMQ 安装步骤，执行：
```bash
sudo apt install -y rabbitmq-server
```

---

### 💡 核心优势
- **稳定性更高**：RabbitMQ 官方维护的 Erlang 源在国内访问更稳定，避免 504 超时问题。
- **兼容性更好**：该源的 Erlang 版本与 RabbitMQ 版本严格适配，减少后续运行时冲突。

需要我给你整理一份**RabbitMQ + Erlang 一键安装脚本**吗？你只需复制到终端运行，就能自动完成源配置、依赖安装和服务启动，避免手动输入出错。

---

## 🔹 第三步：启动与配置 RabbitMQ
### 1. 启动服务并设置开机自启
```bash
# 启动服务
sudo systemctl start rabbitmq-server

# 开机自启
sudo systemctl enable rabbitmq-server

# 检查服务状态
sudo systemctl status rabbitmq-server
```
✅ 看到 `active (running)` 表示服务启动成功。

### 2. 启用 RabbitMQ 管理插件（Web 管理界面）
```bash
sudo rabbitmq-plugins enable rabbitmq_management
```
插件启用后，可通过 `http://虚拟机IP:15672` 访问管理界面（默认端口 `15672`）。

### 3. 创建管理员用户（默认 `guest` 仅允许本地访问）
```bash
# 创建管理员用户（示例：用户名 admin，密码 admin123）
sudo rabbitmqctl add_user admin admin123

# 设置用户为管理员角色
sudo rabbitmqctl set_user_tags admin administrator

# 授予用户所有权限
sudo rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"
```

---

## 🔹 第四步：防火墙与网络配置
### 1. 开放 RabbitMQ 端口（Ubuntu 自带 `ufw` 防火墙）
```bash
# 开放 AMQP 通信端口（5672）
sudo ufw allow 5672/tcp

# 开放 Web 管理界面端口（15672）
sudo ufw allow 15672/tcp

# 重启防火墙生效
sudo ufw reload
```

### 2. 宿主机访问虚拟机管理界面
- 如果虚拟机用 **NAT 模式**：在 VMware 中配置端口转发（将宿主机的 `15672` 端口转发到虚拟机的 `15672` 端口），然后通过 `http://localhost:15672` 访问。
- 如果虚拟机用 **桥接模式**：直接通过虚拟机的 IP（如 `192.168.245.128`）访问 `http://192.168.245.128:15672`。

---

## 🔹 第五步：验证安装
1.  **Web 管理界面验证**：在宿主机浏览器打开管理界面，用刚创建的 `admin` 账号登录，能看到 RabbitMQ 节点状态、队列信息等。
2.  **命令行验证**：
    ```bash
    # 查看节点状态
    sudo rabbitmqctl status

    # 测试队列生产消费（可选）
    rabbitmqadmin declare queue name=test-queue durable=true
    rabbitmqadmin publish routing_key=test-queue payload="Hello RabbitMQ"
    rabbitmqadmin get queue=test-queue
    ```

---

### 💡 常见问题排查
- **Erlang 版本不兼容**：确保使用官方源安装 Erlang，避免 Ubuntu 默认源的旧版本。
- **管理界面无法访问**：检查防火墙端口是否开放、虚拟机网络模式是否允许宿主机访问。
- **服务启动失败**：查看日志 `/var/log/rabbitmq/rabbit@fawei-VMware-Virtual-Platform.log` 定位错误。

需要我给你整理一份**RabbitMQ 常用命令清单**吗？包括队列/交换机管理、用户权限配置、故障排查命令，让你日常运维更高效。