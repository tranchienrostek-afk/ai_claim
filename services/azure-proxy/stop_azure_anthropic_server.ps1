$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PidFile = Join-Path $ProjectRoot "runtime\server.pid"

if (-not (Test-Path $PidFile)) {
    Write-Output "No PID file found."
    exit 0
}

$pidValue = Get-Content $PidFile -ErrorAction SilentlyContinue
if (-not $pidValue) {
    Write-Output "PID file is empty."
    exit 0
}

try {
    Stop-Process -Id ([int]$pidValue) -Force -ErrorAction Stop
    Write-Output ("Stopped process PID {0}" -f $pidValue)
}
catch {
    Write-Output ("Failed to stop PID {0}: {1}" -f $pidValue, $_.Exception.Message)
}
finally {
    Remove-Item $PidFile -ErrorAction SilentlyContinue
}
