# Daily News Insight

一个适合 GitHub Pages 免费托管的 AI 新闻站点模板。

它参考了 `soulmove.github.io/daily-news-ai` 的核心思路，但把结构升级成了更适合你需求的版本：

- 10 个固定新闻板块
- 国内聚合源 + 国际专业媒体 RSS 双通道
- 每日自动抓取原始新闻
- AI 生成板块摘要、利多/利空/观察点、经济分析师点评
- 25 个角色化评论
- 自动归档历史版本
- 前端纯静态，可直接托管在 GitHub Pages

## 目录

- `config/sources.json`: 新闻源、板块和角色配置
- `scripts/daily_digest.py`: 抓取、汇总、生成摘要与归档
- `docs/`: GitHub Pages 站点目录
- `.github/workflows/daily-digest.yml`: GitHub Actions 定时任务

## 技术方案

站点是纯静态页面，数据由 GitHub Actions 每天生成后直接提交回仓库。

1. GitHub Actions 定时运行 `scripts/daily_digest.py`
2. 脚本从 `NewsNow API` 和国际 RSS 拉取新闻
3. 如果存在 `OPENAI_API_KEY`，则调用 OpenAI Responses API 生成正式 AI 版摘要
4. 如果没有密钥，脚本会自动使用模板模式生成可浏览的演示版内容
5. 最新数据写入 `docs/data/latest/digest.json`
6. 历史快照写入 `docs/data/history/YYYY-MM-DD/HH-MM/digest.json`
7. GitHub Pages 直接发布 `docs/`

## 本地运行

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/daily_digest.py
```

运行后可以直接打开 `docs/index.html`，或者在仓库根目录启动一个静态服务器：

```bash
python -m http.server 4173 --directory docs
```

## GitHub Pages 配置

1. 把这个项目推到 GitHub 仓库
2. 在仓库 `Settings -> Pages`
3. 选择 `Deploy from a branch`
4. Branch 选择 `main`
5. Folder 选择 `/docs`

完成后，站点会发布为：

`https://<your-github-name>.github.io/<repo-name>/`

## GitHub Secrets

如果你想启用真实 AI 生成，在仓库 `Settings -> Secrets and variables -> Actions` 里添加：

- `OPENAI_API_KEY`
- `OPENAI_MODEL` 可选，默认 `gpt-5-mini`

没有密钥也能跑，只是会生成模板版内容，方便先把站点和自动化流程搭起来。

## 默认板块

1. 中国宏观
2. 中国市场
3. 全球宏观
4. 地缘政治
5. AI 大模型
6. 芯片与硬件
7. 能源与大宗
8. 加密与 Web3
9. 产业与消费
10. 社会热点

## 备注

- 中文热点源主要走公开聚合接口，适合 GitHub Actions 这类无常驻后端的环境
- 国际媒体以 RSS 为主，稳定且便于长期维护
- 你后面如果要继续扩展，我建议下一步加两个功能：
  - 原始新闻监控页
  - 每个板块独立 SEO 页面

