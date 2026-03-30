# README Image Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复根目录 README 中两张界面预览图片无法显示的问题。

**Architecture:** 本次只修改 `README.md` 的两处图片链接，不改图片资源文件，也不改其他文案。修复方式是删除非标准的 `null` 尾缀，并把路径统一成标准 Markdown 可解析的 `docs/images/...` 相对路径。

**Tech Stack:** Markdown, GitHub-style Markdown rendering

---

### Task 1: 修复 README 图片链接

**Files:**
- Modify: `README.md:63-67`
- Verify: `docs/images/dashboard.png`
- Verify: `docs/images/settings-integrations.png`

- [ ] **Step 1: 读取当前 README 预览图片段**

```markdown
### 仪表盘

![仪表盘](./docs/images/dashboard.png null)

### 全局配置 / 插件管理

![全局配置 / 插件管理](./docs/images/settings-integrations.png null)
```

- [ ] **Step 2: 确认当前写法不符合目标设计**

检查点：
- 链接末尾包含 `null`
- 该写法不是标准 Markdown 图片语法
- 设计要求统一成 `docs/images/...`

Expected: 确认需要替换两处链接。

- [ ] **Step 3: 按设计改为标准 Markdown 图片链接**

将片段改成：

```markdown
### 仪表盘

![仪表盘](docs/images/dashboard.png)

### 全局配置 / 插件管理

![全局配置 / 插件管理](docs/images/settings-integrations.png)
```

- [ ] **Step 4: 验证图片资源路径存在**

检查以下文件是否存在：

```text
docs/images/dashboard.png
docs/images/settings-integrations.png
```

Expected: 两个文件都存在。

- [ ] **Step 5: 重新读取 README 相关片段确认修改结果**

Expected: `README.md` 中对应位置显示为：
- `![仪表盘](docs/images/dashboard.png)`
- `![全局配置 / 插件管理](docs/images/settings-integrations.png)`

- [ ] **Step 6: 提交（仅在用户明确要求提交 git 时执行）**

```bash
git add README.md
git commit -m "docs: fix README image links"
```
