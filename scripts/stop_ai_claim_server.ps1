$ErrorActionPreference = "Stop"

$targets = Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -like "*src.ai_claim.main:app*" -and $_.CommandLine -like "*uvicorn*" }

if (-not $targets) {
  Write-Output "Khong tim thay ai_claim uvicorn process nao."
  exit 0
}

foreach ($proc in $targets) {
  try {
    Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
    Write-Output "Stopped ai_claim PID=$($proc.ProcessId)"
  }
  catch {
    Write-Output "Khong dung duoc PID=$($proc.ProcessId): $($_.Exception.Message)"
  }
}
