param(
    [int]$Port = 0,
    [switch]$Foreground
)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RuntimeDir = Join-Path $ProjectRoot "runtime"
$LogDir = Join-Path $RuntimeDir "logs"
$PidFile = Join-Path $RuntimeDir "server.pid"
$OutLog = Join-Path $LogDir "server.out.log"
$ErrLog = Join-Path $LogDir "server.err.log"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Get-Content (Join-Path $ProjectRoot ".env") -ErrorAction SilentlyContinue | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }
    $k, $v = $_ -split '=', 2
    if (-not $Port -and $k.Trim() -eq "PROXY_PORT") {
        $Port = [int]($v.Trim().Trim('"').Trim("'"))
    }
}

if (-not $Port) {
    $Port = 8009
}

$uvicornArgs = @(
    "-m", "uvicorn",
    "anthropic_compat_server:app",
    "--host", "127.0.0.1",
    "--port", "$Port"
)

if ($Foreground) {
    Push-Location $ProjectRoot
    try {
        python @uvicornArgs
    }
    finally {
        Pop-Location
    }
    return
}

$proc = Start-Process `
    -FilePath python `
    -ArgumentList $uvicornArgs `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError $ErrLog `
    -PassThru

$proc.Id | Set-Content -Path $PidFile -Encoding ascii

Write-Output ("Started Azure Anthropic server on http://127.0.0.1:{0} (PID {1})" -f $Port, $proc.Id)
Write-Output ("PID file: {0}" -f $PidFile)
Write-Output ("Logs: {0}" -f $LogDir)
