param(
  [string]$BindHost = "127.0.0.1",
  [int]$Port = 9780,
  [switch]$Foreground
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $projectRoot "data\\runtime\\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$stdoutLog = Join-Path $logDir "ai_claim_uvicorn.out.log"
$stderrLog = Join-Path $logDir "ai_claim_uvicorn.err.log"

$command = @(
  "-m", "uvicorn",
  "src.ai_claim.main:app",
  "--host", $BindHost,
  "--port", "$Port"
)

if ($Foreground) {
  Push-Location $projectRoot
  try {
    python @command
  }
  finally {
    Pop-Location
  }
  exit 0
}

$existing = Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -like "*src.ai_claim.main:app*" -and $_.CommandLine -like "*uvicorn*" }
if ($existing) {
  Write-Output "ai_claim server da dang chay."
  $existing | Select-Object ProcessId, CommandLine
  exit 0
}

$proc = Start-Process `
  -FilePath "python" `
  -ArgumentList $command `
  -WorkingDirectory $projectRoot `
  -RedirectStandardOutput $stdoutLog `
  -RedirectStandardError $stderrLog `
  -PassThru

Start-Sleep -Seconds 3
Write-Output "Started ai_claim uvicorn PID=$($proc.Id) at http://$BindHost`:$Port"
Write-Output "stdout: $stdoutLog"
Write-Output "stderr: $stderrLog"
