# Any Auto Register

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-blue.svg?style=for-the-badge" alt="Python" />
  <img src="https://img.shields.io/badge/Node.js-18+-green.svg?style=for-the-badge" alt="Node.js" />
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License" />
</p>

<p align="center">
  <a href="README_CN.md">🇨🇳 中文版本</a>
</p>

---

## 🙏 Acknowledgments

This project is a third-generation fork based on the following outstanding open-source projects. We sincerely thank the original authors for their contributions:

- **Original Project (1st Gen)**: [lxf746/any-auto-register](https://github.com/lxf746/any-auto-register) by @lxf746
- **Second Fork (2nd Gen)**: [zc-zhangchen/any-auto-register](https://github.com/zc-zhangchen/any-auto-register) by @zc-zhangchen
- **Temporary Email Solution**: [dreamhunter2333/cloudflare_temp_email](https://github.com/dreamhunter2333/cloudflare_temp_email)

---

## ⚠️ Disclaimer

**This project is for learning and research purposes only. It shall not be used for any commercial or illegal purposes.**

- **Purpose Limitation**: This project is solely for learning and technical research. It shall not be used for any commercial or illegal purposes.
- **Legal Liability**: All consequences arising from the use of this project shall be borne by the user. The author is not responsible for any losses, legal liabilities, or moral disputes caused by the use of this project.
- **Compliance**: Please ensure your use complies with local laws and regulations and the terms of service of each platform.
- **Risk Assumption**: Using this project for account registration may violate the terms of service of relevant platforms. Risks such as account bans and IP bans resulting therefrom shall be borne by the user.
- **Author's Stance**: The author firmly opposes any misuse of this project, including but not limited to mass account registration for fraud, harassment, spam dissemination, and other illegal activities.

---

## 📋 Table of Contents

- [Introduction](#-introduction)
- [Features](#-features)
- [Supported Platforms](#-supported-platforms)
- [Tech Stack](#-tech-stack)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Email Services](#-email-services)
- [Captcha Services](#-captcha-services)
- [Usage Guide](#-usage-guide)
- [Project Structure](#-project-structure)
- [API Documentation](#-api-documentation)
- [Troubleshooting](#-troubleshooting)
- [Development Guide](#-development-guide)
- [Docker Deployment](#-docker-deployment)
- [Contributing](#-contributing)
- [License](#-license)
- [Star History](#-star-history)

---

## 📖 Introduction

**Any Auto Register** is a multi-platform account automatic registration and management system with plugin-based extension, built-in Web UI, and automatic handling of captcha and email verification.

### ✨ Features

- 🎯 **Multi-Platform Support**: ChatGPT, Trae.ai, Cursor, Kiro, Grok, Tavily, OpenBlockLabs, and more
- 🔌 **Plugin Architecture**: Easy to extend new platforms with standardized interfaces
- 📧 **Email Services**: Support for 10+ temporary email and self-hosted email services
- 🤖 **Captcha Handling**: Integrated YesCaptcha and local Turnstile Solver
- 🌐 **Proxy Support**: Built-in proxy pool management for better anonymity
- 📊 **Web UI**: Beautiful and easy-to-use management interface
- 🔄 **Scheduled Tasks**: Support for automatic scheduled registration
- 📈 **Batch Operations**: Support for batch registration (up to 1000 accounts) and batch upload
- ⚡ **Random Delay**: Configurable registration interval with random delay
- 🚀 **One-Click Deployment**: Automated deployment and update scripts

---

## 🌐 Supported Platforms

| Platform | Registration | Token Management | Status Sync | Notes |
|----------|--------------|------------------|-------------|-------|
| **ChatGPT** | ✅ | ✅ | ✅ | Full feature support |
| **Trae.ai** | ✅ | ✅ | ❌ | |
| **Cursor** | ✅ | ✅ | ❌ | |
| **Kiro** | ✅ | ✅ | ❌ | Requires self-hosted email |
| **Grok** | ✅ | ✅ | ❌ | |
| **Tavily** | ✅ | ❌ | ❌ | |
| **OpenBlockLabs** | ✅ | ❌ | ❌ | |

---

## 🛠️ Tech Stack

### Backend
- **Framework**: FastAPI + Uvicorn
- **Database**: SQLite + SQLModel
- **Browser Automation**: Playwright + Camoufox
- **HTTP Client**: curl_cffi + httpx
- **Task Scheduling**: APScheduler

### Frontend
- **Framework**: React + TypeScript
- **UI Library**: Ant Design
- **Build Tool**: Vite
- **State Management**: Zustand

### Infrastructure
- **Container**: Docker + Docker Compose
- **Environment**: Conda (recommended) or venv

---

## 🚀 Quick Start

### Prerequisites

- **Python**: 3.12 or higher
- **Node.js**: 18 or higher
- **Conda**: Recommended for environment management
- **Git**: For cloning the repository

### Method 1: One-Click Deployment (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/dsclca12/auto_reg.git
cd auto_reg

# 2. Run deployment script
./deploy.sh
```

After deployment, access http://localhost:8000

### Method 2: Manual Installation

#### 1. Clone the Repository
```bash
git clone https://github.com/dsclca12/auto_reg.git
cd auto_reg
```

#### 2. Create Python Environment
```bash
# Using Conda (recommended)
conda create -n any-auto-register python=3.12 -y
conda activate any-auto-register

# Or using venv
python3 -m venv any-auto-register-env
source any-auto-register-env/bin/activate  # Linux/Mac
```

#### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 4. Install Browsers
```bash
python -m playwright install chromium
python -m camoufox fetch
```

#### 5. Install Frontend Dependencies
```bash
cd frontend
npm install
npm run build
cd ..
```

#### 6. Configure Environment Variables
```bash
cp .env.example .env
# Edit .env file with your configuration
```

#### 7. Start the Service
```bash
python main.py
```

Access http://localhost:8000

---

## ⚙️ Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure as needed:

```bash
# Server Configuration
HOST=0.0.0.0
PORT=8000
APP_RELOAD=0
APP_CONDA_ENV=any-auto-register

# Captcha Services
YESCAPTCHA_CLIENT_KEY=your_client_key
LOCAL_SOLVER_URL=http://127.0.0.1:8889

# Proxy (Optional)
PROXY_URL=http://username:password@ip:port

# Email Services (Configure based on your needs)
MOEMAIL_API_KEY=your_api_key
SKYMAIL_API_KEY=your_api_key
SKYMAIL_DOMAIN=your_domain
```

### Email Services

| Service | Identifier | Description | Configuration Required |
|---------|------------|-------------|----------------------|
| **LuckMail** | `luckmail` | Temporary email service with API-based access | Yes |
| **MoeMail** | `moemail` | Default option, auto-register temporary email | Yes |
| **TempMail.lol** | `tempmail_lol` | Temporary email, may require proxy in some regions | No |
| **SkyMail (CloudMail)** | `skymail` | Use via API/Token/Domain | Yes |
| **YYDS Mail / MaliAPI** | `maliapi` | Support domain and auto-domain strategy | Yes |
| **GPTMail** | `gptmail` | Generate temporary email via GPTMail API | Yes |
| **DuckMail** | `duckmail` | Temporary email service | Yes |
| **Freemail** | `freemail` | Self-hosted email service | Yes |
| **Laoudo** | `laoudo` | Fixed email service | Yes |
| **CF Worker** | `cfworker` | Self-hosted Cloudflare Worker email | Yes |

#### 📧 Kiro Email Requirements

Kiro has strict risk control. Email solution significantly affects success rate:

- **Self-hosted email**: 100% success rate ✅
- **Built-in temporary email**: 0% success rate ❌

**Recommendation**: Use self-hosted email (CF Worker, SkyMail) for Kiro registration.

### Captcha Services

| Service | Description | Configuration |
|---------|-------------|---------------|
| **YesCaptcha** | Third-party captcha solving service | Requires Client Key |
| **Local Solver** | Built-in Turnstile solver (camoufox + quart) | Auto-starts with backend |

### External System Integration

| System | Description | Configuration |
|--------|-------------|---------------|
| **CPA** | Codex Protocol API management panel | API URL + Key |
| **Sub2API** | API transit management | API URL + Key |
| **Team Manager** | Team management | - |
| **grok2api** | Grok token management | API URL + Key |

---

## 📚 Usage Guide

### Register Accounts

1. Visit **Register Task** page
2. Select platform and configuration
3. Set batch quantity (max 1000) and delay
4. Click **Start Registration**

### Scheduled Tasks

1. Visit **Scheduled Tasks** page
2. Create task and set execution time
3. Supports one-time and recurring execution
4. System will automatically execute at scheduled time
5. Supports pause/resume

### Batch Upload

1. Visit **Account Management**
2. Select platform
3. Select accounts (all or specific)
4. Click **Batch Upload** to Sub2API/CPA

### ChatGPT Token Modes

The frontend provides two ChatGPT registration modes:

| Mode | Description | Output | Recommendation |
|------|-------------|--------|----------------|
| **With Refresh Token** | Uses new PR flow | Access Token + Refresh Token | ✅ Recommended |
| **Without Refresh Token** | Legacy flow | Access Token / Session only | ⚠️ RT-dependent features unavailable |

**Location**: Registration Task Page or ChatGPT Platform Registration Dialog

### ChatGPT Batch Operations

Available at the top of ChatGPT platform list:

- **Status Sync**
  - Sync selected accounts' local status
  - Sync selected accounts' CLIProxyAPI status
  - Or batch execute on current filter results

- **Re-upload Undiscovered**
  - Re-upload auth-file not found remotely
  - Supports "current filter scope" or "currently selected accounts"

---

## 📁 Project Structure

```
auto_reg/
├── api/                    # API routes
│   ├── accounts.py        # Account management APIs
│   ├── tasks.py           # Task management APIs
│   ├── platforms.py       # Platform APIs
│   ├── proxies.py         # Proxy management APIs
│   ├── config.py          # Configuration APIs
│   ├── actions.py         # Action APIs
│   └── integrations.py    # External integration APIs
├── core/                   # Core logic
│   ├── db.py              # Database initialization
│   ├── registry.py        # Platform registry
│   ├── scheduler.py       # Task scheduler
│   └── config_store.py    # Configuration storage
├── platforms/              # Platform plugins
│   ├── chatgpt/           # ChatGPT platform
│   ├── trae/              # Trae.ai platform
│   ├── cursor/            # Cursor platform
│   ├── kiro/              # Kiro platform
│   └── ...                # Other platforms
├── services/               # Service layer
│   ├── email_services/    # Email service implementations
│   ├── solver_manager.py  # Captcha solver management
│   └── chatgpt_sync.py    # ChatGPT sync service
├── frontend/               # Frontend code
│   ├── src/               # Source code
│   └── dist/              # Build artifacts
├── static/                 # Frontend build output
├── scripts/                # Utility scripts
├── docker/                 # Docker configuration
├── main.py                 # Entry point
├── requirements.txt        # Python dependencies
├── deploy.sh               # One-click deployment script
├── update.sh               # Quick update script
├── .env.example            # Configuration example
└── README.md               # Project documentation
```

---

## 📡 API Documentation

After starting the service, access http://localhost:8000/docs for interactive API documentation (Swagger UI).

### Key Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/accounts` | GET/POST | Account management |
| `/api/tasks` | GET/POST | Task management |
| `/api/platforms` | GET | List supported platforms |
| `/api/proxies` | GET/POST | Proxy management |
| `/api/config` | GET/PUT | Configuration management |
| `/api/actions` | POST | Execute actions |
| `/api/integrations` | GET/POST | External integrations |
| `/api/solver/status` | GET | Solver status |
| `/api/solver/restart` | POST | Restart solver |

---

## 🔧 Troubleshooting

### Turnstile Solver Not Running

**Symptoms**: Captcha verification fails, solver status shows offline

**Solutions**:
1. Check if backend is started correctly
2. Ensure running in correct Python environment (Conda env recommended)
3. Verify camoufox is installed: `python -m camoufox fetch`
4. Check solver logs in `backend.log`

### Port Occupied

**Symptoms**: Service fails to start, port 8000 already in use

**Solutions**:
```bash
# Stop existing service
pkill -f "python main.py"

# Or find and kill specific process
lsof -i :8000
kill <PID>

# Restart service
python main.py
```

### Email Service Failure

**Symptoms**: Cannot receive verification codes

**Solutions**:
1. Check proxy configuration and network connection
2. Some services require proxy access
3. Verify API keys are correct
4. Try alternative email service

### Registration Disallowed (ChatGPT)

**Error**: `registration_disallowed` or HTTP 400

**Solutions**:
1. 🔄 **Change proxy IP** (current IP may be flagged, use residential proxy)
2. 📧 **Change email provider** (temporary email domains may be blacklisted)
3. ⏱️ **Reduce registration frequency** (add 30-60s random delay)
4. 🔃 **Clear browser data** or change device fingerprint
5. 📋 **Reduce batch size** (max 5 accounts per batch recommended)

### Registration Quantity Limit

- Maximum: 1000 accounts per batch
- Recommended: Use random delay (10-30s)
- Best Practice: 5-10 accounts per batch with 30-60s delay

### TLS/SSL Errors

**Symptoms**: Connection errors during registration

**Solutions**:
1. Check proxy availability
2. Update dependencies: `pip install -r requirements.txt --upgrade`
3. Reinstall browsers: `python -m playwright install chromium`

---

## 🛠️ Development Guide

### Add New Platform

1. Create new platform plugin in `platforms/` directory
2. Implement `BasePlatform` interface
3. Register with `@register` decorator

Example:
```python
from core.registry import register, BasePlatform

@register
class MyPlatform(BasePlatform):
    name = "my_platform"
    display_name = "My Platform"
    
    async def register(self, config):
        # Implementation
        pass
```

### Frontend Development

```bash
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev

# Access http://localhost:5173

# Build for production
npm run build
```

### Backend Development

```bash
# Activate Conda environment
conda activate any-auto-register

# Start with auto-reload
export APP_RELOAD=1
python main.py
```

### Running Tests

```bash
pytest tests/
```

---

## 🐳 Docker Deployment

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+

### Quick Start

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop service
docker-compose down
```

### Environment Variables

```bash
# Configure in docker-compose.yml or .env
SOLVER_BROWSER_TYPE=camoufox
CLIPROXYAPI_PORT_BIND=8317
GROK2API_PORT_BIND=8011
```

### Volume Mounts

| Host Path | Container Path | Description |
|-----------|----------------|-------------|
| `./data` | `/runtime` | Runtime data |
| `./_ext_targets` | `/_ext_targets` | External targets |
| `./external_logs` | `/app/services/external_logs` | External logs |

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

### Before Contributing
1. Ensure code follows project conventions
2. Remove any sensitive information
3. Follow the original project's open source license

### How to Contribute
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Reporting Issues
- Use GitHub Issues for bug reports and feature requests
- Provide detailed information including:
  - Steps to reproduce
  - Expected behavior
  - Actual behavior
  - Environment information (OS, Python version, etc.)

---

## 📄 License

MIT License

See [LICENSE](LICENSE) file for details.

---

## 📊 Star History

[![Star History Chart](https://api.star-history.com/svg?repos=dsclca12/auto_reg&type=Date)](https://star-history.com/#dsclca12/auto_reg&Date)

---

## 📞 Contact

- 📧 **Email**: dev@example.com
- 💬 **Issues**: [GitHub Issues](https://github.com/dsclca12/auto_reg/issues)
- 🌟 **Repository**: [GitHub](https://github.com/dsclca12/auto_reg)

---

## 👥 Authors

- **Current Maintainer**: [@dsclca12](https://github.com/dsclca12)
- **Original Author**: [@lxf746](https://github.com/lxf746)
- **Second Fork Author**: [@zc-zhangchen](https://github.com/zc-zhangchen)

---

<p align="center">
  <strong>⚠️ Reminder: Please use this project legally and responsibly. The author is not responsible for any misuse.</strong>
</p>
