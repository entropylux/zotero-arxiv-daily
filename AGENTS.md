# Project guidance

## Purpose and current operation
Recommend papers from the Zotero corpus, rank all candidates, and email at most 20.
This fork uses Windows local scheduling; operational authority is LOCAL_WINDOWS.md.
The maintained branch is main. GitHub email delivery is manual-only.

## Stack and commands
Python, Hydra/OmegaConf, PyTorch/sentence-transformers, OpenAI-compatible API, SMTP.
On this machine use PowerShell and the designated interpreter:

```powershell
$pythonPath = 'C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe'
& $pythonPath scripts/run_local.py --check
$env:PYTHONPATH = "$PWD\src;$PWD\.cuda-deps;$PWD\.local-deps"
& $pythonPath -S -m pytest tests -q -p no:cacheprovider
```

`--check` is offline. `--preview` calls online services but sends no email.
A normal run sends email; `--force` bypasses daily deduplication.
CI uses uv and includes slow tests; Windows local execution uses the Compose entry.

## Conventions and boundaries
Keep credentials in ignored .env.local; never print or commit them.
Preserve dependencies, model cache, logs, and daily success state.
Use src before dependency overlays; CUDA overlay precedes CPU dependencies.
Keep ranking before arXiv full-text extraction; bound network concurrency.
Preserve ranked order and abstract fallback when full-text extraction fails.
Use pytest stubs for external services; keep integration tests explicitly slow.
Use loguru in application modules; launcher console output is intentional.
Do not infer delivery success from a preview or a scheduler exit code alone.
Update LOCAL_WINDOWS.md when commands, configuration, or runtime behavior change.
