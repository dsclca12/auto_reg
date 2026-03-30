# README 图片显示修复设计

## 背景
根目录 `README.md` 的“界面预览”部分有两处图片链接无法正常显示。

当前写法为：
- `![仪表盘](./docs/images/dashboard.png null)`
- `![全局配置 / 插件管理](./docs/images/settings-integrations.png null)`

问题在于链接末尾附带了 `null`，这不是标准 Markdown 图片语法，容易导致 GitHub、IDE Markdown 预览或其他渲染器解析失败。

## 目标
在不调整图片资源、不改动其他 README 内容的前提下，修复根目录 `README.md` 中两张预览图的显示问题。

## 方案
采用最小范围的文档修复：

1. 只修改根目录 `README.md`
2. 将两处图片链接改为标准 Markdown 语法
3. 同时把路径统一为相对仓库根目录更直观的 `docs/images/...`

修改后目标写法：
- `![仪表盘](docs/images/dashboard.png)`
- `![全局配置 / 插件管理](docs/images/settings-integrations.png)`

## 备选方案对比
### 方案 A：仅删除 `null`
- 优点：改动最小
- 缺点：仍保留 `./docs/...`，一致性一般

### 方案 B：删除 `null` 并统一路径写法（推荐）
- 优点：同时修复显示问题并规范链接写法
- 缺点：比方案 A 多一点点文本改动，但无额外风险

## 影响范围
- 修改文件：`README.md`
- 不涉及：`docs/images/` 下图片资源、前后端代码、构建流程、测试逻辑

## 验证方式
修复后应确认：
1. `README.md` 中两张图片链接语法为标准 Markdown
2. `docs/images/dashboard.png` 和 `docs/images/settings-integrations.png` 路径存在
3. 在 GitHub 风格 Markdown 预览或本地 IDE 预览中，两张图片可正常显示

## 风险
风险极低。本次仅为文档链接修复，不影响运行时代码。