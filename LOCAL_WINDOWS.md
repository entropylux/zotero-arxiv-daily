# Windows local delivery

Daily runs use the upstream Zotero-ranking pipeline, with local configuration
in `config/local.yaml`, ignored credentials in `.env.local`, and isolated
dependencies in `.local-deps` plus `.cuda-deps`.

Run an offline preflight:

```powershell
& 'C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe' scripts\run_local.py --check
```

Register and enable the 09:00 local-time task:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
scripts\register_local_task.ps1 -Enable
```

The task requires the current Windows user to be signed in. Logs are written
to `logs/local`; daily duplicate protection is stored in `.local-state/status.json`.
Use `scripts/run_local.py --force` only for an intentional same-day rerun.
`--check` never contacts Zotero, the model API, or SMTP.

The GitHub Actions workflow remains available for manual dispatch; its daily
schedule is disabled so local delivery is the only automatic run.
