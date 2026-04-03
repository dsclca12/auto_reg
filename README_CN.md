# Any Auto Register

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-blue.svg?style=for-the-badge" alt="Python" />
  <img src="https://img.shields.io/badge/Node.js-18+-green.svg?style=for-the-badge" alt="Node.js" />
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License" />
</p>

<p align="center">
  <a href="README.md">🇬🇧 English Version</a>
</p>

---

## 🙏 致谢

本项目是在以下优秀开源项目基础上的三开版本，在此衷心感谢原作者们的贡献：

- **一开项目**：[lxf746/any-auto-register](https://github.com/lxf746/any-auto-register) by @lxf746
- **二开项目**：[zc-zhangchen/any-auto-register](https://github.com/zc-zhangchen/any-auto-register) by @zc-zhangchen
- **临时邮箱方案**：[dreamhunter2333/cloudflare_temp_email](https://github.com/dreamhunter2333/cloudflare_temp_email)

本项目在前作基础上进行了功能扩展和优化。

---

## ⚠️ 免责声明

**请务必在使用本项目前仔细阅读以下声明：**

1. **用途限制**：本项目仅供学习和技术研究使用，不得用于任何商业用途或非法用途。
2. **法律责任**：使用本项目所产生的一切后果由使用者自行承担。作者不对因使用本项目而导致的任何损失、法律责任或道德纠纷负责。
3. **合规使用**：请确保您的使用行为符合当地法律法规以及各平台的服务条款。
4. **风险自担**：使用本项目进行账号注册可能违反相关平台的服务条款，由此导致的账号封禁、IP 封禁等风险由使用者自行承担。
5. **作者立场**：本项目作者坚决反对任何滥用本项目的行为，包括但不限于批量注册账号进行诈骗、骚扰、垃圾信息传播等违法行为。

---

## 📋 目录

- [项目简介](#-项目简介)
- [功能特性](#-功能特性)
- [支持平台](#-支持平台)
- [技术栈](#-技术栈)
- [快速开始](#-快速开始)
- [配置说明](#-配置说明)
- [邮箱服务](#-邮箱服务)
- [验证码服务](#-验证码服务)
- [使用指南](#-使用指南)
  - [注册账号](#注册账号)
  - [定时任务](#定时任务)
  - [批量上传](#批量上传)
  - [删除账号](#删除账号)
  - [一键更新](#一键更新)
- [项目结构](#-项目结构)
- [API 文档](#-api-文档)
- [常见问题](#-常见问题)
- [开发指南](#-开发指南)
- [Docker 部署](#-docker-部署)
- [贡献指南](#-贡献指南)
- [许可证](#-许可证)
- [Star History](#-star-history)

---

## 📖 项目简介

**Any Auto Register** 是一个多平台账号自动注册与管理系统，支持插件化扩展，内置 Web UI，可自动处理验证码和邮箱验证。

### ✨ 功能特性

- 🎯 **多平台支持**：ChatGPT、Trae.ai、Cursor、Kiro、Grok、Tavily、OpenBlockLabs 等
- 🔌 **插件化架构**：易于通过标准化接口扩展新平台
- 📧 **邮箱服务**：支持 10+ 种临时邮箱和自建邮箱服务
- 🤖 **验证码处理**：集成 YesCaptcha 和本地 Turnstile Solver
- 🌐 **代理支持**：内置代理池管理，更好的匿名性
- 📊 **Web 管理界面**：美观易用的管理后台
- 🔄 **定时任务**：支持定时自动注册
- 📈 **批量操作**：支持批量注册（最多 1000 个）和批量上传
- ⚡ **随机延迟**：可配置的注册间隔随机延迟
- 🚀 **一键部署**：自动化部署和更新脚本

---

## 🌐 支持平台

| 平台 | 注册 | Token 管理 | 状态同步 | 说明 |
|------|------|-----------|---------|------|
| **ChatGPT** | ✅ | ✅ | ✅ | 完整功能支持 |
| **Trae.ai** | ✅ | ✅ | ❌ | |
| **Cursor** | ✅ | ✅ | ❌ | |
| **Kiro** | ✅ | ✅ | ❌ | 需使用自建邮箱 |
| **Grok** | ✅ | ✅ | ❌ | |
| **Tavily** | ✅ | ❌ | ❌ | |
| **OpenBlockLabs** | ✅ | ❌ | ❌ | |

---

## 🛠️ 技术栈

### 后端
- **框架**: FastAPI + Uvicorn
- **数据库**: SQLite + SQLModel
- **浏览器自动化**: Playwright + Camoufox
- **HTTP 客户端**: curl_cffi + httpx
- **任务调度**: APScheduler

### 前端
- **框架**: React + TypeScript
- **UI 库**: Ant Design
- **构建工具**: Vite
- **状态管理**: Zustand

### 基础设施
- **容器化**: Docker + Docker Compose
- **环境管理**: Conda（推荐）或 venv

---

## 🚀 快速开始

### 环境要求

- **Python**: 3.12 或更高版本
- **Node.js**: 18 或更高版本
- **Conda**: 推荐用于环境管理
- **Git**: 用于克隆仓库

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
# 使用 Conda（推荐）
conda create -n any-auto-register python=3.12 -y
conda activate any-auto-register

# 或使用 venv
python3 -m venv any-auto-register-env
source any-auto-register-env/bin/activate  # Linux/Mac
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

## ⚙️ 配置说明

### 环境变量

复制 `.env.example` 到 `.env` 并按需配置：

```bash
# 服务器配置
HOST=0.0.0.0
PORT=8000
APP_RELOAD=0
APP_CONDA_ENV=any-auto-register

# 验证码服务
YESCAPTCHA_CLIENT_KEY=your_client_key
LOCAL_SOLVER_URL=http://127.0.0.1:8889

# 代理（可选）
PROXY_URL=http://username:password@ip:port

# 邮箱服务（根据需要配置）
MOEMAIL_API_KEY=your_api_key
SKYMAIL_API_KEY=your_api_key
SKYMAIL_DOMAIN=your_domain
```

### 邮箱服务

| 服务 | 标识 | 说明 | 需要配置 |
|------|------|------|---------|
| **LuckMail** | `luckmail` | 基于 API 的临时邮箱服务 | 是 |
| **MoeMail** | `moemail` | 默认选项，自动注册临时邮箱 | 是 |
| **TempMail.lol** | `tempmail_lol` | 临时邮箱，部分地区可能需要代理 | 否 |
| **SkyMail (CloudMail)** | `skymail` | 通过 API/Token/域名使用 | 是 |
| **YYDS Mail / MaliAPI** | `maliapi` | 支持域名和自动域名策略 | 是 |
| **GPTMail** | `gptmail` | 通过 GPTMail API 生成临时邮箱 | 是 |
| **DuckMail** | `duckmail` | 临时邮箱服务 | 是 |
| **Freemail** | `freemail` | 自建邮箱服务 | 是 |
| **Laoudo** | `laoudo` | 固定邮箱服务 | 是 |
| **CF Worker** | `cfworker` | 自建 Cloudflare Worker 邮箱 | 是 |

#### 📧 Kiro 邮箱要求

Kiro 风控严格，邮箱方案显著影响成功率：

- **自建邮箱**：100% 成功率 ✅
- **内置临时邮箱**：0% 成功率 ❌

**建议**：Kiro 注册使用自建邮箱（CF Worker、SkyMail）。

### 验证码服务

| 服务 | 说明 | 配置 |
|------|------|------|
| **YesCaptcha** | 第三方验证码解决服务 | 需要 Client Key |
| **本地 Solver** | 内置 Turnstile 解决器（camoufox + quart） | 随后端自动启动 |

### 外部系统集成

| 系统 | 说明 | 配置 |
|------|------|------|
| **CPA** | Codex Protocol API 管理面板 | API URL + Key |
| **Sub2API** | API 中转管理 | API URL + Key |
| **Team Manager** | 团队管理 | - |
| **grok2api** | Grok token 管理 | API URL + Key |

---

## 📚 使用指南

### 注册账号

1. 访问 **注册任务** 页面
2. 选择平台和配置
3. 设置批量数量（最大 1000 个）和延迟
4. 点击 **开始注册**

### 定时任务

1. 访问 **定时任务** 页面
2. 创建任务并设置执行时间
3. 支持单次和循环执行
4. 系统会在预定时间自动执行
5. 支持暂停/恢复

### 批量上传

1. 访问 **账号管理**
2. 选择平台
3. 选择账号（全选或指定）
4. 点击 **批量上传** 到 Sub2API/CPA

### ChatGPT Token 模式

前端提供两种 ChatGPT 注册模式：

| 模式 | 说明 | 输出 | 推荐 |
|------|------|------|------|
| **带 Refresh Token** | 使用新 PR 流程 | Access Token + Refresh Token | ✅ 推荐 |
| **不带 Refresh Token** | 旧流程 | 仅 Access Token / Session | ⚠️ RT 相关功能不可用 |

**位置**：注册任务页面或 ChatGPT 平台注册对话框

### ChatGPT 批量操作

在 ChatGPT 平台列表顶部可用：

- **状态同步**
  - 同步所选账号本地状态
  - 同步所选账号 CLIProxyAPI 状态
  - 或对当前筛选结果批量执行

- **补传远端未发现**
  - 补传远端未发现的 auth-file
  - 支持"当前筛选范围"或"当前所选账号"两种作用范围

### 删除账号

1. 访问 **账号管理** 页面
2. 选择要删除的账号（可单选或批量选择）
3. 点击 **删除** 按钮确认删除
4. 支持批量删除选中的多个账号

### 一键更新

项目提供了快速更新脚本，可一键完成代码拉取、依赖更新和服务重启：

```bash
# 在项目根目录下执行
./update.sh
```

脚本会自动完成以下操作：
1. 停止当前运行的服务
2. 拉取最新代码
3. 更新 Python 依赖
4. 更新前端依赖并重新构建
5. 询问是否立即启动服务

> **注意**：脚本默认使用 Conda 环境。如使用 venv，请先激活环境后手动执行更新步骤。

---

## 📁 项目结构

```
auto_reg/
├── api/                    # API 路由
│   ├── accounts.py        # 账号管理 API
│   ├── tasks.py           # 任务管理 API
│   ├── platforms.py       # 平台 API
│   ├── proxies.py         # 代理管理 API
│   ├── config.py          # 配置 API
│   ├── actions.py         # 操作 API
│   └── integrations.py    # 外部集成 API
├── core/                   # 核心逻辑
│   ├── db.py              # 数据库初始化
│   ├── registry.py        # 平台注册表
│   ├── scheduler.py       # 任务调度器
│   └── config_store.py    # 配置存储
├── platforms/              # 平台插件
│   ├── chatgpt/           # ChatGPT 平台
│   ├── trae/              # Trae.ai 平台
│   ├── cursor/            # Cursor 平台
│   ├── kiro/              # Kiro 平台
│   └── ...                # 其他平台
├── services/               # 服务层
│   ├── email_services/    # 邮箱服务实现
│   ├── solver_manager.py  # 验证码解决器管理
│   └── chatgpt_sync.py    # ChatGPT 同步服务
├── frontend/               # 前端代码
│   ├── src/               # 源代码
│   └── dist/              # 构建产物
├── static/                 # 前端构建输出
├── scripts/                # 工具脚本
├── docker/                 # Docker 配置
├── main.py                 # 入口文件
├── requirements.txt        # Python 依赖
├── deploy.sh               # 一键部署脚本
├── update.sh               # 快速更新脚本
├── .env.example            # 配置示例
└── README.md               # 项目文档
```

---

## 📡 API 文档

启动服务后访问 http://localhost:8000/docs 查看交互式 API 文档（Swagger UI）。

### 主要端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/accounts` | GET/POST | 账号管理 |
| `/api/tasks` | GET/POST | 任务管理 |
| `/api/platforms` | GET | 列出支持的平台 |
| `/api/proxies` | GET/POST | 代理管理 |
| `/api/config` | GET/PUT | 配置管理 |
| `/api/actions` | POST | 执行操作 |
| `/api/integrations` | GET/POST | 外部集成 |
| `/api/solver/status` | GET | Solver 状态 |
| `/api/solver/restart` | POST | 重启 Solver |

---

## 🔧 常见问题

### Turnstile Solver 未运行

**症状**：验证码验证失败，Solver 状态显示离线

**解决方案**：
1. 检查后端是否正确启动
2. 确保在正确的 Python 环境中运行（推荐 Conda 环境）
3. 验证 camoufox 已安装：`python -m camoufox fetch`
4. 查看 `backend.log` 中的 Solver 日志

### 端口被占用

**症状**：服务启动失败，端口 8000 已被占用

**解决方案**：
```bash
# 停止现有服务
pkill -f "python main.py"

# 或查找并终止特定进程
lsof -i :8000
kill <PID>

# 重启服务
python main.py
```

### 邮箱服务失败

**症状**：无法接收验证码

**解决方案**：
1. 检查代理配置和网络连接
2. 部分服务需要代理访问
3. 验证 API Key 是否正确
4. 尝试其他邮箱服务

### 注册被拒绝（ChatGPT）

**错误**：`registration_disallowed` 或 HTTP 400

**解决方案**：
1. 🔄 **更换代理 IP**（当前 IP 可能被标记，建议使用住宅代理）
2. 📧 **更换邮箱服务商**（临时邮箱域名可能已被拉黑）
3. ⏱️ **降低注册频率**（增加 30-60 秒随机延迟）
4. 🔃 **清除浏览器数据**或更换设备指纹
5. 📋 **减少批量大小**（建议每批最多 5 个账号）

### 注册数量限制

- 最大值：每批 1000 个账号
- 建议：使用随机延迟（10-30 秒）
- 最佳实践：每批 5-10 个账号，延迟 30-60 秒

### TLS/SSL 错误

**症状**：注册期间连接错误

**解决方案**：
1. 检查代理是否可用
2. 更新依赖：`pip install -r requirements.txt --upgrade`
3. 重新安装浏览器：`python -m playwright install chromium`

---

## 🛠️ 开发指南

### 添加新平台

1. 在 `platforms/` 目录创建新平台插件
2. 实现 `BasePlatform` 接口
3. 使用 `@register` 装饰器注册

示例：
```python
from core.registry import register, BasePlatform

@register
class MyPlatform(BasePlatform):
    name = "my_platform"
    display_name = "My Platform"
    
    async def register(self, config):
        # 实现代码
        pass
```

### 前端开发

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev

# 访问 http://localhost:5173

# 生产环境构建
npm run build
```

### 后端开发

```bash
# 激活 Conda 环境
conda activate any-auto-register

# 启动自动重载
export APP_RELOAD=1
python main.py
```

### 运行测试

```bash
pytest tests/
```

---

## 🐳 Docker 部署

### 环境要求

- Docker 20.10+
- Docker Compose 2.0+

### 快速开始

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

### 环境变量

```bash
# 在 docker-compose.yml 或 .env 中配置
SOLVER_BROWSER_TYPE=camoufox
CLIPROXYAPI_PORT_BIND=8317
GROK2API_PORT_BIND=8011
```

### 卷挂载

| 主机路径 | 容器路径 | 说明 |
|---------|---------|------|
| `./data` | `/runtime` | 运行时数据 |
| `./_ext_targets` | `/_ext_targets` | 外部目标 |
| `./external_logs` | `/app/services/external_logs` | 外部日志 |

---

## 🤝 贡献指南

欢迎贡献！请遵循以下指南：

### 贡献前
1. 确保代码符合项目规范
2. 不包含任何敏感信息
3. 遵循原项目的开源协议

### 如何贡献
1. Fork 本仓库
2. 创建特性分支（`git checkout -b feature/amazing-feature`）
3. 提交更改（`git commit -m 'Add amazing feature'`）
4. 推送到分支（`git push origin feature/amazing-feature`）
5. 提交 Pull Request

### 报告问题
- 使用 GitHub Issues 报告 bug 和提出功能请求
- 请提供详细信息，包括：
  - 复现步骤
  - 预期行为
  - 实际行为
  - 环境信息（操作系统、Python 版本等）

---

## 📄 许可证

**MIT License**

详见 [LICENSE](LICENSE) 和 [NOTICE](NOTICE) 文件。

### 版权所有者
- Copyright (c) 2024-present dsclca12（当前维护者）
- Copyright (c) 2024 zc-zhangchen（二开作者）
- Copyright (c) 2024 lxf746（一开作者）

### 上游项目
- [lxf746/any-auto-register](https://github.com/lxf746/any-auto-register) - 一开项目（MIT）
- [zc-zhangchen/any-auto-register](https://github.com/zc-zhangchen/any-auto-register) - 二开项目（MIT）

### 附加条款
本项目仅供**学习和技术研究使用**，不得用于任何商业用途或非法用途。完整免责声明见 [LICENSE](LICENSE) 文件。

---

## 📊 Star History

[![Star History Chart](https://api.star-history.com/svg?repos=dsclca12/auto_reg&type=Date)](https://star-history.com/#dsclca12/auto_reg&Date)

---

## 📞 联系方式

- 📧 **邮箱**：dev@example.com
- 💬 **Issues**：[GitHub Issues](https://github.com/dsclca12/auto_reg/issues)
- 🌟 **仓库**：[GitHub](https://github.com/dsclca12/auto_reg)

---

## 👥 作者

- **当前维护者**：[@dsclca12](https://github.com/dsclca12)
- **一开作者**：[@lxf746](https://github.com/lxf746)
- **二开作者**：[@zc-zhangchen](https://github.com/zc-zhangchen)

---

<p align="center">
  <strong>⚠️ 再次提醒：请合法合规使用本项目，作者不对任何滥用行为负责</strong>
</p>
