# Any Auto Register

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-blue.svg?style=for-the-badge" alt="Python" />
  <img src="https://img.shields.io/badge/Node.js-18+-green.svg?style=for-the-badge" alt="Node.js" />
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License" />
</p>

---

## ⚠️ 免责声明

**请务必在使用本项目前仔细阅读以下声明：**

1. **用途限制**：本项目仅供学习和技术研究使用，不得用于任何商业用途或非法用途。

2. **法律责任**：使用本项目所产生的一切后果由使用者自行承担。作者不对因使用本项目而导致的任何损失、法律责任或道德纠纷负责。

3. **合规使用**：请确保您的使用行为符合当地法律法规以及各平台的服务条款。

4. **风险自担**：使用本项目进行账号注册可能违反相关平台的服务条款，由此导致的账号封禁、IP 封禁等风险由使用者自行承担。

5. **作者立场**：本项目作者坚决反对任何滥用本项目的行为，包括但不限于批量注册账号进行诈骗、骚扰、垃圾信息传播等违法行为。

---

## 项目简介

多平台账号自动注册与管理系统，支持插件化扩展，内置 Web UI，可自动处理验证码和邮箱验证。

### 功能特性

- 🎯 **多平台支持**：ChatGPT, Trae.ai, Cursor, Kiro, Grok, Tavily, OpenBlockLabs
- 🔌 **插件化架构**：易于扩展新平台
- 📧 **邮箱服务**：支持多种临时邮箱和自建邮箱服务
- 🤖 **验证码处理**：集成 YesCaptcha 和本地 Solver
- 🌐 **代理支持**：内置代理池管理
- 📊 **Web 管理界面**：美观易用的管理后台
- 🔄 **定时任务**：支持定时自动注册
- 📈 **批量操作**：支持批量注册和批量上传（最大 1000 个）
- ⚡ **随机延迟**：支持注册间隔随机延迟
- 🚀 **一键部署**：支持自动化部署和更新

---

## 🙏 致谢

本项目是在以下优秀开源项目基础上的三开版本，在此衷心感谢原作者们的贡献：

- **一开项目**：[lxf746/any-auto-register](https://github.com/lxf746/any-auto-register) by @lxf746
- **二开项目**：[zc-zhangchen/any-auto-register](https://github.com/zc-zhangchen/any-auto-register) by @zc-zhangchen
- **临时邮箱方案**：[dreamhunter2333/cloudflare_temp_email](https://github.com/dreamhunter2333/cloudflare_temp_email)

本项目在前作基础上进行了功能扩展和优化。

---

## 🚀 快速开始

### 环境要求

- Python 3.12+
- Node.js 18+
- Conda（推荐）或 venv
- Git

### 方法一：一键部署（推荐）

```bash
# 1. 克隆项目
git clone https://github.com/dsclca12/auto_reg.git
cd auto_reg

# 2. 执行部署脚本
./deploy.sh
```

部署完成后访问 http://localhost:8000

### 方法二：手动安装

#### 1. 克隆项目
```bash
git clone https://github.com/dsclca12/auto_reg.git
cd auto_reg
```

#### 2. 创建 Python 环境
```bash
conda create -n auto-reg python=3.12 -y
conda activate auto-reg
```

或使用 venv：
```bash
python3 -m venv auto-reg-env
source auto-reg-env/bin/activate  # Linux/Mac
```

#### 3. 安装依赖
```bash
pip install -r requirements.txt
```

#### 4. 安装浏览器
```bash
python -m playwright install chromium
python -m camoufox fetch
```

#### 5. 安装前端依赖
```bash
cd frontend
npm install
npm run build
cd ..
```

#### 6. 配置环境变量
```bash
cp .env.example .env
# 编辑 .env 文件，填入你的配置
```

#### 7. 启动服务
```bash
python main.py
```

访问 http://localhost:8000

---

## 更新项目

使用快速更新脚本：

```bash
cd auto_reg
./update.sh
```

或手动更新：

```bash
git pull origin main
source auto-reg-env/bin/activate
pip install -r requirements.txt -q
cd frontend && npm install && npm run build
cd ..
pkill -f "python main.py"
python main.py &
```

---

## 配置说明

### 邮箱服务

| 服务 | 说明 | 配置要求 |
|------|------|----------|
| MoeMail | 推荐默认，自动注册临时账号 | 是 |
| Laoudo | 适合固定邮箱场景 | 是 |
| CF Worker | 基于 Cloudflare Worker 自建 | 是 |
| TempMail.lol | 自动生成，无需配置 | 否 |
| DuckMail | 临时邮箱 | 是 |

### 验证码服务

- **YesCaptcha**: 需填写 Client Key
- **本地 Solver**: 依赖 camoufox + quart，自动拉起

### 外部系统集成

- **CPA**: Codex Protocol API 管理面板
- **Sub2API**: API 中转管理
- **Team Manager**: 团队管理
- **grok2api**: Grok token 管理

---

## 使用指南

### 注册账号

1. 访问 **注册任务** 页面
2. 选择平台和配置
3. 设置批量数量（最大 1000 个）
4. 设置固定延迟和随机延迟
5. 点击开始注册

### 定时任务

1. 访问 **定时任务** 页面
2. 创建任务并设置执行时间
3. 支持单次执行和循环执行
4. 系统会自动执行

### 批量上传

1. 访问 **账号管理**
2. 选择平台
3. 使用全选或手动勾选账号
4. 点击批量上传到 Sub2API/CPA

---

## 项目结构

```
auto_reg/
├── api/              # API 路由
├── core/             # 核心逻辑
├── platforms/        # 平台插件
├── services/         # 服务层
├── frontend/         # 前端代码
├── static/           # 前端构建产物
├── main.py           # 入口文件
├── requirements.txt  # Python 依赖
├── deploy.sh         # 一键部署脚本
├── update.sh         # 快速更新脚本
├── .env.example      # 配置示例
└── README.md         # 项目说明
```

---

## API 文档

启动服务后访问 http://localhost:8000/docs

---

## 常见问题

### Turnstile Solver 未运行

检查后端是否正确启动，确保在正确的 Python 环境中运行。

### 端口被占用

```bash
# 停止服务
pkill -f "python main.py"
# 重新启动
python main.py
```

### 邮箱服务失败

检查代理配置和网络连接，部分服务需要代理访问。

### 注册数量限制

最大支持 1000 个账号批量注册，建议配合随机延迟使用。

---

## 开发指南

### 添加新平台

1. 在 `platforms/` 目录创建新平台插件
2. 实现 `BasePlatform` 接口
3. 使用 `@register` 装饰器注册

### 前端开发

```bash
cd frontend
npm run dev
# 访问 http://localhost:5173
```

---

## 许可证

MIT License

详见 [LICENSE](LICENSE) 文件。

---

## 联系方式

如有问题或建议，请通过以下方式联系：

- 📧 Email: `dev@example.com`
- 💬 Issues: [GitHub Issues](https://github.com/dsclca12/auto_reg/issues)

---

## 贡献

欢迎提交 Issue 和 Pull Request！

在提交前请确保：
1. 代码符合项目规范
2. 不包含任何敏感信息
3. 遵循原项目的开源协议

---

<p align="center">
  <strong>⚠️ 再次提醒：请合法合规使用本项目，作者不对任何滥用行为负责</strong>
</p>

---

## 作者

[@dsclca12](https://github.com/dsclca12) - 原作者和维护者

---
