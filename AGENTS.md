# Local project notes

The application code follows TideDra's upstream `main` branch. The only
intentional source change is the local reranker's CUDA auto-selection,
immutable model revision, and CPU fallback on GPU out-of-memory.

The Windows launchers and `config/local.yaml` support the user's local 09:00
digest. Keep credentials in ignored `.env.local`; preserve `.local-state`,
`.cache`, `.local-deps`, and `.cuda-deps`. Never include them in commits.

Use the configured interpreter at
`C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe`.
