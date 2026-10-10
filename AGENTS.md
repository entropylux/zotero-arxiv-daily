# Local project rules

Read LOCAL_WINDOWS.md for the active deployment. Use Python at
`C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe`.

Ranking depends only on Zotero abstracts, with upstream recent-addition decay.
Do not reintroduce manual interests, author/institution bonuses, or novelty review.
Default daily maximum: 20. Summaries use abstracts; no full-text downloads in the active arXiv pipeline.

Credentials load through scripts/run_local.py from ignored .env.local. Never print or commit credentials.
Use --preview for verification without sending; do not force or resend uncertain deliveries.
Preserve security controls, delivery records, caches, and historical archives.
