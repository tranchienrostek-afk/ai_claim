param(
    [ValidateSet("azure", "glm", "custom")]
    [string]$Provider = "azure",
    [string]$Model = "sonnet",
    [string]$Prompt = "",
    [switch]$Print,
    [switch]$Bare,
    [string]$BaseUrl = "",
    [string]$SonnetAlias = "",
    [string]$OpusAlias = ""
)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Port = 8009

if (-not $BaseUrl) {
    Get-Content (Join-Path $ProjectRoot ".env") -ErrorAction SilentlyContinue | ForEach-Object {
        if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }
        $k, $v = $_ -split '=', 2
        if ($k.Trim() -eq "PROXY_PORT") {
            $script:Port = [int]($v.Trim().Trim('"').Trim("'"))
        }
    }
    $BaseUrl = "http://127.0.0.1:$Port"
}

if (-not $SonnetAlias) {
    switch ($Provider) {
        "glm" { $SonnetAlias = "glm-sonnet" }
        "custom" { $SonnetAlias = "sonnet" }
        default { $SonnetAlias = "azure-sonnet" }
    }
}

if (-not $OpusAlias) {
    switch ($Provider) {
        "glm" { $OpusAlias = "glm-opus" }
        "custom" { $OpusAlias = "opus" }
        default { $OpusAlias = "azure-opus" }
    }
}

$env:ANTHROPIC_API_KEY = "local-anthropic-proxy"
$env:ANTHROPIC_BASE_URL = $BaseUrl
$env:ANTHROPIC_DEFAULT_SONNET_MODEL = $SonnetAlias
$env:ANTHROPIC_DEFAULT_OPUS_MODEL = $OpusAlias

$claudeArgs = @()
if ($Print) { $claudeArgs += "-p" }
if ($Bare) { $claudeArgs += "--bare" }
$claudeArgs += @("--model", $Model)
if ($Prompt) { $claudeArgs += $Prompt }

Write-Output ("Using ANTHROPIC_BASE_URL={0}" -f $BaseUrl)
Write-Output ("Using provider={0}" -f $Provider)
Write-Output ("Using model alias: sonnet={0}, opus={1}" -f $SonnetAlias, $OpusAlias)
Write-Output ("Running: claude {0}" -f ($claudeArgs -join " "))

& claude @claudeArgs
