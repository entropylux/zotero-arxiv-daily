param([switch]$Enable)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = 'C:\Users\ASUS\AppData\Local\Python\pythoncore-3.14-64\python.exe'
$runnerPath = Join-Path $PSScriptRoot 'run_local.py'
$taskName = 'Zotero arXiv Daily - Local'

if ($Enable) {
    & $pythonPath $runnerPath --check
    if ($LASTEXITCODE -ne 0) { throw 'Local preflight failed; task not enabled.' }
}
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw "Task already exists: $taskName. Inspect it before replacing."
}
$arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}"' -f (Join-Path $PSScriptRoot 'run_scheduled.ps1')
$action = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument $arguments -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger -Daily -At '09:00'
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 2)
if (-not $Enable) { $settings.Enabled = $false }
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description 'Daily top 20 Zotero-related arXiv papers at 09:00 local time. Requires user signed in; logs stored in project/logs/local.'
Register-ScheduledTask -TaskName $taskName -InputObject $task | Select-Object TaskName, State
