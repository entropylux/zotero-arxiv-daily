# Windows 本地运行

程序、模型缓存和定时任务在本机运行；Zotero、arXiv、摘要模型 API 和邮箱仍使用在线服务。
每天北京时间 09:00，按 Zotero 相关性选取最多 20 篇。配置位于 `config/local.yaml`，
沿用 `Daily/**`、七个物理分类及中文摘要。

## 凭据

复制 `.env.local.example` 为 `.env.local`，在本地填写空白项。
`ZOTERO_ID` 是数字用户 ID，`SENDER_PASSWORD` 是邮箱 SMTP 授权码。
`OPENAI_API_BASE`、`OPENAI_API_KEY` 和 `LLM_MODEL` 必须来自同一可用模型服务；
原服务曾返回余额不足，需确认有额度。代理未运行时，请清空 `ARXIV_DAILY_PROXY` 或启动代理。
`.env.local`、依赖、缓存和运行记录均已加入 Git 忽略规则。

## 命令（PowerShell，在项目目录执行）

```powershell
$pythonPath = 'C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe'
# 首次安装；依赖仅写入项目目录。需要代理时为 pip 添加 --proxy 参数。
& $pythonPath -m pip install --target .local-deps -r requirements-local.txt

# 离线配置及导入检查，不发邮件。
& $pythonPath scripts/run_local.py --check

# 正常运行，会调用在线服务并发送邮件。
& $pythonPath scripts/run_local.py
```

日志位于 `logs/local/`，最近一次执行状态位于 `.local-state/status.json`。
成功执行过的当天自动跳过重复运行；明确需要再次推送时添加 `--force`。
同一时间只允许一个本地实例运行。模型下载缓存保留在 `.cache/huggingface/`。

## 定时任务

任务名称：`Zotero arXiv Daily - Local`。每天 09:00（Windows 系统时区）。
默认创建为禁用状态。配置和实际运行验证通过后：

```powershell
Enable-ScheduledTask -TaskName 'Zotero arXiv Daily - Local'
Get-ScheduledTaskInfo -TaskName 'Zotero arXiv Daily - Local'
```

任务以当前 Windows 用户的普通权限、隐藏窗口运行，不保存 Windows 密码。
需要用户已登录；锁屏可以运行，注销或关机不能运行。
设置了允许电池运行、请求唤醒和错过时间后补跑，实际唤醒取决于电脑电源设置。
最长运行 2 小时，不自动重试，避免发送状态不明确时重复发信。

新机器上运行 `scripts/register_local_task.ps1` 可创建禁用任务；
添加 `-Enable` 会先做配置检查，然后创建启用任务。已有同名任务时脚本拒绝覆盖。

迁移顺序：填写凭据 → 离线检查 → 本地实际运行验证 → 启用本地定时任务 →
移除 GitHub `.github/workflows/main.yml` 的 `schedule`（保留手动触发）。
本地未验证通过之前，保留 GitHub 日程以避免中断推送。
