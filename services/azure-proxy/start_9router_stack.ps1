param(
    [switch]$SkipNpmInstall,
    [switch]$SkipAzureProxy,
    [switch]$Foreground,
    [int]$RouterPort = 0
)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ProjectRoot
$RouterRoot = Join-Path $RepoRoot "9router"
$RuntimeDir = Join-Path $ProjectRoot "runtime"
$LogDir = Join-Path $RuntimeDir "logs"
$PidFile = Join-Path $RuntimeDir "9router.pid"
$OutLog = Join-Path $LogDir "9router.out.log"
$ErrLog = Join-Path $LogDir "9router.err.log"
$StatePath = Join-Path $RuntimeDir "9router_stack.json"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$bootstrapArgs = @("bootstrap_9router_stack.py")
if ($RouterPort) {
    $bootstrapArgs += @("--router-port", "$RouterPort")
}

Push-Location $ProjectRoot
try {
    python @bootstrapArgs
}
finally {
    Pop-Location
}

if (-not (Test-Path $StatePath)) {
    throw "Missing stack state: $StatePath"
}

$state = Get-Content $StatePath -Raw | ConvertFrom-Json

function Test-HttpOk {
    param(
        [string]$Url,
        [hashtable]$Headers = @{}
    )
    try {
        Invoke-RestMethod -Uri $Url -Headers $Headers -Method Get -TimeoutSec 5 | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

if (-not $SkipAzureProxy) {
    if (-not (Test-HttpOk -Url ($state.azure_proxy_base_url + "/health"))) {
        Write-Output "Azure local proxy is not healthy; starting it now..."
        powershell -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot "start_azure_anthropic_server.ps1") | Out-Host
    }
}

if (-not $SkipNpmInstall -and -not (Test-Path (Join-Path $RouterRoot "node_modules"))) {
    Write-Output "Installing 9router dependencies..."
    Push-Location $RouterRoot
    try {
        & npm.cmd install
        if ($LASTEXITCODE -ne 0) {
            throw "npm install failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

if ($Foreground) {
    Push-Location $RouterRoot
    try {
        & npm.cmd run dev
    }
    finally {
        Pop-Location
    }
    return
}

$proc = Start-Process `
    -FilePath "npm.cmd" `
    -ArgumentList @("run", "dev") `
    -WorkingDirectory $RouterRoot `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError $ErrLog `
    -PassThru

$proc.Id | Set-Content -Path $PidFile -Encoding ascii

$healthy = $false
$routerAuthHeaders = @{ "x-api-key" = $state.router_api_key }
for ($i = 0; $i -lt 90; $i++) {
    Start-Sleep -Seconds 1
    if (Test-HttpOk -Url ($state.router_base_url + "/v1/models") -Headers $routerAuthHeaders) {
        $healthy = $true
        break
    }
}

if (-not $healthy) {
    Write-Warning "9router did not become healthy in time. Check logs:"
    Write-Warning $OutLog
    Write-Warning $ErrLog
    exit 1
}

Write-Output ("Started 9router on {0} (PID {1})" -f $state.router_base_url, $proc.Id)
Write-Output ("Router API key saved in {0}" -f $StatePath)
Write-Output ("Logs: {0}" -f $LogDir)
