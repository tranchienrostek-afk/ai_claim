param(
    [switch]$IncludeAzureProxy
)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RuntimeDir = Join-Path $ProjectRoot "runtime"
$PidFile = Join-Path $RuntimeDir "9router.pid"

if (Test-Path $PidFile) {
    $routerPid = (Get-Content $PidFile -Raw).Trim()
    if ($routerPid) {
        try {
            Stop-Process -Id ([int]$routerPid) -Force -ErrorAction Stop
            Write-Output ("Stopped 9router PID {0}" -f $routerPid)
        }
        catch {
            Write-Warning ("Could not stop PID {0}: {1}" -f $routerPid, $_.Exception.Message)
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}
else {
    Write-Output "9router PID file not found."
}

if ($IncludeAzureProxy) {
    powershell -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot "stop_azure_anthropic_server.ps1")
}
