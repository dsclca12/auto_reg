[📖 中文版本](README_CN.md)


# Any Auto Register


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

### Installation

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

## 🙏 Acknowledgments

This project is a third-generation fork based on the following outstanding open-source projects. We sincerely thank the original authors for their contributions:

- **Original Project (1st Gen)**: [lxf746/any-auto-register](https://github.com/lxf746/any-auto-register) by @lxf746
- **Second Fork (2nd Gen)**: [zc-zhangchen/any-auto-register](https://github.com/zc-zhangchen/any-auto-register) by @zc-zhangchen

This project builds upon the work of previous generations with improvements and optimizations.

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

See [LICENSE](LICENSE) file for details.

---

## Contributing

Issues and Pull Requests are welcome!

Before submitting, please ensure:
1. Code follows project conventions
2. No sensitive information is included
3. Follows the original project's open source license

---

## Support

If you have any questions, please submit an Issue or contact the author.

---

