[🇹🇼 中文版本](README_CN.md)


# Any Auto Register

---

## 🙏 Acknowledgments

This project is a third-generation fork based on the following outstanding open-source projects. We sincerely thank the original authors for their contributions:

- **Original Project (1st Gen)**: [lxf746/any-auto-register](https://github.com/lxf746/any-auto-register) by @lxf746
- **Second Fork (2nd Gen)**: [zc-zhangchen/any-auto-register](https://github.com/zc-zhangchen/any-auto-register) by @zc-zhangchen

- [项目简介](#项目简介)
- [当前界面与实际平台展示](#当前界面与实际平台展示)
- [功能特性](#功能特性)
- [界面预览](#界面预览)
- [技术栈](#技术栈)
- [环境要求](#环境要求)
- [ChatGPT 专项能力](#chatgpt-专项能力)
- [邮箱服务支持](#邮箱服务支持)
- [快速开始](#快速开始)
- [Docker 部署](#docker-部署)
- [插件与外部依赖](#插件与外部依赖)
- [常见问题排查](#常见问题排查)
- [项目结构](#项目结构)
- [Electron 开发说明](#electron-开发说明)
- [License](#license)
- [Star History](#star-history)

---

## ⚠️ Disclaimer

**This project is for learning and research purposes only. It shall not be used for any commercial or illegal purposes.**

All consequences arising from the use of this project shall be borne by the user. The author is not responsible for any losses, legal liabilities, or moral disputes caused by the use of this project.

---

## Introduction

Multi-platform account automatic registration and management system, supporting plugin-based extension, built-in Web UI, and automatic handling of captcha and email verification.

### Features

- 🎯 **Multi-Platform Support**: ChatGPT, Trae.ai, Cursor, Kiro, Grok, Tavily, OpenBlockLabs
- 🔌 **Plugin Architecture**: Easy to extend new platforms
- 📧 **Email Services**: Support for multiple temporary email and self-hosted email services
- 🤖 **Captcha Handling**: Integrated YesCaptcha and local Solver
- 🌐 **Proxy Support**: Built-in proxy pool management
- 📊 **Web UI**: Beautiful and easy-to-use management interface
- 🔄 **Scheduled Tasks**: Support for automatic scheduled registration
- 📈 **Batch Operations**: Support for batch registration and batch upload

---

## Quick Start

### Requirements

- Python 3.12+
- Node.js 18+
- Conda (recommended) or venv

## ChatGPT 专项能力

当前版本里，**ChatGPT 是功能最完整的平台之一**，不仅支持注册，还支持 Token 生命周期管理、状态探测和外部系统同步。

### 1. ChatGPT Token 方案切换

前端当前提供两种 ChatGPT 注册模式：

- **有 RT**（默认推荐）
  - 走新 PR 链路
  - 产出 **Access Token + Refresh Token**
- **无 RT**（兼容旧方案）
  - 走旧链路
  - 仅产出 **Access Token / Session**
  - 依赖 RT 的后续能力可能不可用

这项切换在以下位置都能看到：

- 注册任务页
- ChatGPT 平台注册弹窗



### 4. ChatGPT 批量状态同步与补传

在 ChatGPT 平台列表顶部，当前还有两类批量能力：

- **状态同步**
  - 同步所选账号本地状态
  - 同步所选账号 CLIProxyAPI 状态
  - 或对当前筛选结果批量执行
- **补传远端未发现**
  - 补传远端未发现的 auth-file
  - 支持“当前筛选范围”或“当前所选账号”两种作用范围

## 邮箱服务支持

根据当前注册页实际配置项，项目支持以下邮箱服务：

| 服务名称 | 标识 | 说明 |
| --- | --- | --- |
| LuckMail | `luckmail` | 可免费领取 **125 个邮箱**用于测试，且**每天签到还能继续领取邮箱**；可通过 [https://mails.luckyous.com/9331211B](https://mails.luckyous.com/9331211B) 进入，支持博主获得少量赏金，用于维持开源测试 |
| MoeMail | `moemail` | 默认常用方案，自动注册账号并生成邮箱 |
| TempMail.lol | `tempmail_lol` | 临时邮箱方案，部分地区可能需要代理 |
| SkyMail (CloudMail) | `skymail` | 通过 API / Token / 域名使用 |
| YYDS Mail / MaliAPI | `maliapi` | 支持域名与自动域名策略 |
| GPTMail | `gptmail` | 基于 GPTMail API 生成临时邮箱并轮询邮件，支持已知域名时本地拼装随机地址 |
| DuckMail | `duckmail` | 临时邮箱方案 |
| Freemail | `freemail` | 自建邮箱服务 |
| Laoudo | `laoudo` | 固定邮箱方案 |
| CF Worker | `cfworker` | Cloudflare Worker 自建邮箱 |

### Kiro 邮箱说明

Kiro 当前风控较严格，邮箱方案会显著影响成功率。当前项目内也保留了这条重点提示：

- **自建邮箱成功率：100%**
- **项目内置临时邮箱成功率：0%**

因此进行 **Kiro (AWS Builder ID)** 注册时，建议优先使用**自建邮箱**。

## 快速开始

### 1. 创建并激活 Conda 环境

1. **Clone the repository**
```bash
git clone https://github.com/dsclca12/auto_reg.git
cd auto_reg
```

2. **Create Python environment**
```bash
conda create -n auto-reg python=3.12 -y
conda activate auto-reg
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Install browsers**
```bash
python -m playwright install chromium
python -m camoufox fetch
```

5. **Install frontend dependencies**
```bash
cd frontend
npm install
npm run build
cd ..
```

6. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env file with your configuration
```

7. **Start the service**
```bash
python main.py
```

Access http://localhost:8000

---

## Configuration

### Email Services

| Service | Description | Configuration Required |
|---------|-------------|----------------------|
| MoeMail | Recommended default, auto-register temporary email | Yes |
| Laoudo | Suitable for fixed email scenarios | Yes |
| CF Worker | Self-hosted based on Cloudflare Worker | Yes |
| TempMail.lol | Auto-generated, no configuration needed | No |
| DuckMail | Temporary email | Yes |

### Captcha Services

- **YesCaptcha**: Requires Client Key
- **Local Solver**: Depends on camoufox + quart, auto-starts with backend

### External System Integration

- **CPA**: Codex Protocol API management panel
- **Sub2API**: API transit management
- **Team Manager**: Team management
- **grok2api**: Grok token management

---

## Usage Guide

### Register Accounts

1. Visit **Register Task** page
2. Select platform and configuration
3. Set batch quantity and delay
4. Click Start Registration

### Scheduled Tasks

1. Visit **Scheduled Tasks** page
2. Create task and set execution time
3. System will automatically execute
4. Supports pause/resume

### Batch Upload

1. Visit **Account Management**
2. Select platform
3. Check accounts
4. Click Batch Upload

---

## Project Structure

```
auto_reg/
├── api/              # API routes
├── core/             # Core logic
├── platforms/        # Platform plugins
├── services/         # Service layer
├── frontend/         # Frontend code
├── static/           # Frontend build artifacts
├── main.py           # Entry point
├── requirements.txt  # Python dependencies
├── .env.example      # Configuration example
└── README.md         # Project documentation
```

---

## API Documentation

Access http://localhost:8000/docs after starting the service

---

## Common Issues

### Turnstile Solver Not Running

Check if backend is started correctly and ensure it's running in the correct Python environment.

### Port Occupied

```bash
# Stop service
pkill -f "python main.py"
# Restart
python main.py
```

### Email Service Failure

Check proxy configuration and network connection. Some services require proxy access.

### Registration Quantity Limit

Maximum supports 1000 accounts per batch registration, recommended to use with random delay.

---

## Development Guide

### Add New Platform

1. Create new platform plugin in `platforms/` directory
2. Implement `BasePlatform` interface
3. Register with `@register` decorator

### Frontend Development

```bash
cd frontend
npm run dev
# Access http://localhost:5173
```

---

## Author

[@dsclca12](https://github.com/dsclca12) - Original author and maintainer

## License

MIT License

## 用户讨论群

- QQ群：**1065114376**（any-auto-register 注册机用户讨论群）

## Star History

