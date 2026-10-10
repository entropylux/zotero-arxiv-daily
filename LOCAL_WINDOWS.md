# 本地文献库精选

2026-10-10 恢复到 TideDra/zotero-arxiv-daily 的
`edc68632c9a861559ea376dfd34c3ab0b1e4f34a`，保留本地运行和安全修复。

## 当前流程

Zotero `Daily/**` 文献摘要 → arXiv Atom 新文章 → 文献库相似度排序 → 最多 20 篇 → 中文 TLDR 和相关度评分。
近期加入文献库的文章按上游算法获得较高权重。没有兴趣描述、作者、师门、机构或创新性加分。
评分衡量相关性，不衡量论文质量；先以数量上限控制篇幅，尚未验证精选效果改善。
只对选中文章的摘要生成 TLDR，两路并行；不下载全文、不核查前作。
摘要服务失败时显示原文摘要。没有新文章时不发邮件。

## 运行

解释器：`C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe`
入口：`scripts/run_local.py`。`--check` 离线检查，`--preview` 联网预览但不发邮件。
凭据仅从 `.env.local` 读取。配置在 `config/local.yaml`。
每天北京时间 09:00 由当前 Codex 自动化启动；依赖本机和 Codex 可运行。

正式报告：`.local-state/report.json`；预览：`.local-state/preview-report.json` 和 `preview.html`。
当天成功记录：`.local-state/status.json`；发送去重：`.local-state/delivery.json`。
同一 arXiv ID 的不同版本默认不重复发送。旧取证流程的已发记录自动继承。
pending 表示发送结果不明确，须核实邮箱后人工处理，不能自动重发。

保留 SMTP TLS 验证、固定模型版本/禁用远程代码、HTML 转义、资源上限和固定 Action 版本。
旧源代码及 Git 历史备份在项目父目录 `backups/before-upstream-20261010-202140`。
旧取证档案留在 `.local-state/curation`，不再参与筛选。
