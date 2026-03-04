param(
    [string]$ProjectRoot = "",
    [int]$Port = 8501,
    [int]$RestartDelaySeconds = 5,
    [int]$HealthCheckIntervalSeconds = 20,
    [switch]$PreventSleep
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$artifactDir = Join-Path $ProjectRoot "artifacts"
if (-not (Test-Path $artifactDir)) {
    New-Item -ItemType Directory -Path $artifactDir -Force | Out-Null
}

$outLog = Join-Path $artifactDir "streamlit.out.log"
$errLog = Join-Path $artifactDir "streamlit.err.log"
$runnerLog = Join-Path $artifactDir "always_on_runner.log"
$healthUrl = "http://127.0.0.1:$Port"

function Write-RunnerLog {
    param([string]$Message)
    $line = "$(Get-Date -Format s)  $Message"
    Add-Content -Path $runnerLog -Value $line
    Write-Host $line
}

if ($PreventSleep) {
    # Keep system/display awake while this process is running.
    Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class NativePower {
    [DllImport("kernel32.dll")]
    public static extern uint SetThreadExecutionState(uint esFlags);
}
"@
    $ES_CONTINUOUS = 0x80000000
    $ES_SYSTEM_REQUIRED = 0x00000001
    $ES_DISPLAY_REQUIRED = 0x00000002
    [void][NativePower]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED -bor $ES_DISPLAY_REQUIRED)
    Write-RunnerLog "PreventSleep enabled."
}

function Start-Streamlit {
    Write-RunnerLog "Starting Streamlit on port $Port"
    Start-Process -FilePath "python" `
        -ArgumentList "-m streamlit run app.py --server.port $Port --server.address 0.0.0.0" `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -PassThru
}

function Test-Health {
    try {
        $resp = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 4
        return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500)
    } catch {
        return $false
    }
}

Write-RunnerLog "Always-on runner booting. Root=$ProjectRoot Port=$Port"

while ($true) {
    if (Test-Health) {
        Write-RunnerLog "Healthy server already detected on port $Port. Monitoring only."
        Start-Sleep -Seconds $HealthCheckIntervalSeconds
        continue
    }

    $proc = Start-Streamlit
    Start-Sleep -Seconds 4
    Write-RunnerLog "Streamlit PID=$($proc.Id)"

    while (-not $proc.HasExited) {
        Start-Sleep -Seconds $HealthCheckIntervalSeconds
        if (-not (Test-Health)) {
            Write-RunnerLog "Health check failed. Restarting PID=$($proc.Id)"
            try { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue } catch {}
            break
        }
    }

    if (Test-Health) {
        Write-RunnerLog "Another healthy instance is active on port $Port. Skipping restart cycle."
        Start-Sleep -Seconds $HealthCheckIntervalSeconds
        continue
    }

    if ($proc.HasExited) {
        Write-RunnerLog "Process exited with code $($proc.ExitCode). Restarting in $RestartDelaySeconds sec."
    }
    Start-Sleep -Seconds $RestartDelaySeconds
}
