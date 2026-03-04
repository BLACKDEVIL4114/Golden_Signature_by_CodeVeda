param(
    [string]$TaskName = "AGPO-Streamlit-AlwaysOn",
    [int]$Port = 8501,
    [switch]$PreventSleep
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runnerPath = Join-Path $PSScriptRoot "always_on_runner.ps1"

if (-not (Test-Path $runnerPath)) {
    throw "Runner script not found: $runnerPath"
}

$preventSleepArg = ""
if ($PreventSleep) { $preventSleepArg = " -PreventSleep" }

$psArgs = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runnerPath`" -ProjectRoot `"$projectRoot`" -Port $Port$preventSleepArg"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $psArgs
$triggerStartup = New-ScheduledTaskTrigger -AtStartup
$triggerLogin = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($triggerStartup, $triggerLogin) `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Installed scheduled task: $TaskName"
Write-Host "Starts on startup + login and keeps Streamlit on port $Port alive."
