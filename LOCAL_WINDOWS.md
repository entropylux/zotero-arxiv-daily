# Windows 本地运行

程序、模型缓存和定时任务在本机运行；Zotero、arXiv、摘要模型 API 和邮箱仍使用在线服务。
每天北京时间 09:00，按 Zotero 相关性选取最多 20 篇。配置位于 `config/local.yaml`，
沿用 `Daily/**`、七个物理分类及中文摘要。

## 凭据

复制 `.env.local.example` 为 `.env.local`，在本地填写空白项。
`ZOTERO_ID` 是数字用户 ID，`SENDER_PASSWORD` 是邮箱 SMTP 授权码。
`OPENAI_API_BASE`、`OPENAI_API_KEY` 和 `LLM_MODEL` 必须来自同一可用模型服务；
需确认所选接口接受该 Key 且有可用额度。代理未运行时，请清空 `ARXIV_DAILY_PROXY` 或启动代理。
`.env.local`、依赖、缓存和运行记录均已加入 Git 忽略规则。

本机已验证的智谱组合：`OPENAI_API_BASE=https://open.bigmodel.cn/api/coding/paas/v4`、
`LLM_MODEL=glm-5.3-flash`、`LLM_API_MODE=chat_completion`。
这是 Coding Plan 路由；普通 API 路由 `/api/paas/v4` 使用不同的额度体系。

## 命令（PowerShell，在项目目录执行）

```powershell
$pythonPath = 'C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe'
# 首次安装；依赖仅写入项目目录。需要代理时为 pip 添加 --proxy 参数。
& $pythonPath -m pip install --target .local-deps -r requirements-local.txt

# 可选：NVIDIA GPU 加速（需兼容 CUDA 13.2 的驱动）。独立覆盖 CPU 版 torch。
& $pythonPath -m pip install --target .cuda-deps --no-deps -r requirements-cuda.txt

# 离线配置及导入检查，不发邮件。
& $pythonPath scripts/run_local.py --check

# 正常运行，会调用在线服务并发送邮件。
& $pythonPath scripts/run_local.py

# 完整性能验证：调用在线服务生成预览，不发送邮件，也不改变当天成功标记。
& $pythonPath scripts/run_local.py --preview
```

日志位于 `logs/local/`，最近一次执行状态位于 `.local-state/status.json`。
定时任务通过 `scripts/run_scheduled.ps1` 启动，启动时间和退出码记录在
`logs/local/task-launch.log`。没有当天业务日志时先检查该文件及任务的 `LastTaskResult`；
任务退出码为 0 也可能表示当天已完成而跳过，发送结果应结合业务日志和成功日期判断。
本地入口通过 Hydra Compose API 加载配置，避开 Hydra 命令行解析器与 Python 3.14 的兼容性问题。
成功执行过的当天自动跳过重复运行；明确需要再次推送时添加 `--force`。
同一时间只允许一个本地实例运行。模型下载缓存保留在 `.cache/huggingface/`。

## 性能配置

arXiv 流程为：全部 Atom 元数据 → 摘要相关性排序 → 选取前 20 → 提取全文 → 生成摘要。
排序输入仍是全部候选摘要；不再下载未入选论文的全文。
本地 `executor.fulltext_workers: 3`、`executor.llm_workers: 4` 分别控制全文和模型请求的并发数。
线程重叠网络等待，全文解析使用独立子进程；完成顺序不改变最终邮件排名。
模型服务的并发限制仍然有效，出现限流时应降低 `llm_workers`。

`reranker.local.device: auto` 优先使用 CUDA，没有可用 CUDA 时使用 CPU。
日志会显示实际计算设备和每阶段秒数。GPU 只负责本地摘要向量计算，在线摘要生成仍由智谱处理。
`.cuda-deps` 优先于 `.local-deps`，不需要替换全局 Python 包。
预览结果位于 `.local-state/preview.html`，性能数据位于 `.local-state/preview-metrics.json`。

### 本机实测（2026-09-15）

Ryzen 9 9950X3D / RTX 5080 16GB，183 篇候选，85 篇 Zotero 参考文献，选取 20 篇。

| 阶段 | 原流程 | 优化后预览 |
| --- | ---: | ---: |
| 全文处理 | 全部 183 篇，982 秒 | 仅前 20 篇，24 秒 |
| 模型摘要及机构 | 串行，396 秒 | 4 并发，101 秒 |
| 排序（包括模型启动） | CPU，约 33 秒 | CUDA，34 秒 |

优化后处理阶段共 166 秒；原完整运行约 1450 秒（包括约 32 秒邮件发送）。
预览不发邮件，网络时延和模型负载也会变化，因此不把两次总时间视为严格同条件基准。
20 篇均获得全文、非降级摘要及机构解析结果，未出现模型限流。
主要收益来自减少全文任务量和重叠网络等待；小批量排序受模型初始化影响，CUDA 不保证总阶段更快。
同一批输入的 CPU/CUDA 对照入选集合相同，BF16 运算使最大分数差约 0.0093，近分数论文顺序可能不同。

## 定时任务

当前调度（2026-09-17）：Codex 桌面端自动化“每日 arXiv 前20篇推送”（`arxiv-20`），
每天北京时间 09:00 在本机运行入口并核对发送结果。需要本机及 Codex 可运行、网络和代理可用；
这不是关机后仍能运行的云端任务。Windows 任务已停用，避免重复调度。
下述 Windows 注册方式仅作为备用，不要与 Codex 自动化同时启用。

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

Windows 调度已于 2026-09-17 停用，由上面的 Codex 自动化接管。
当天实际邮件成功记录见 `.local-state/status.json`；优化后的预览记录独立保存，不能当作发送凭证。
GitHub `.github/workflows/main.yml` 已移除 `schedule`，保留手动触发。
`keep-alive.yml` 仍有仓库保活日程，不负责发送邮件。
新机器仍需完成凭据填写、检查和实际运行验证，再启用本地任务。
