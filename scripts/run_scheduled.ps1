$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$logDirectory = Join-Path $projectRoot 'logs\local'
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
$launchLog = Join-Path $logDirectory 'task-launch.log'
$pythonPath = 'C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe'
$exitCode = 1
try {
    Add-Content -LiteralPath $launchLog -Encoding UTF8 -Value "$(Get-Date -Format o) Starting scheduled runner"
    Set-Location -LiteralPath $projectRoot
    & $pythonPath (Join-Path $PSScriptRoot 'run_local.py') 2>&1 | Out-File -LiteralPath $launchLog -Append -Encoding UTF8
    $exitCode = $LASTEXITCODE
} catch {
    Add-Content -LiteralPath $launchLog -Encoding UTF8 -Value "$(Get-Date -Format o) Launcher failed: $($_.Exception.GetType().Name)"
} finally {
    Add-Content -LiteralPath $launchLog -Encoding UTF8 -Value "$(Get-Date -Format o) Exit code: $exitCode"
}
exit $exitCode

