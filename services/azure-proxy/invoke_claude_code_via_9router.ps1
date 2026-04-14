param(
    [ValidateSet("combo", "azure", "glm")]
    [string]$Mode = "combo",
    [string]$Model = "sonnet",
    [string]$Prompt = "",
    [switch]$Print,
    [switch]$Bare
)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$StatePath = Join-Path $ProjectRoot "runtime\\9router_stack.json"

if (-not (Test-Path $StatePath)) {
    throw "Missing stack state. Run start_9router_stack.ps1 first."
}

$state = Get-Content $StatePath -Raw | ConvertFrom-Json

switch ($Mode) {
    "azure" {
        $sonnetAlias = "azure-sonnet"
        $opusAlias = "azure-opus"
    }
    "glm" {
        $sonnetAlias = "glm-sonnet"
        $opusAlias = "glm-opus"
    }
    default {
        $sonnetAlias = $state.router_sonnet_model
        $opusAlias = $state.router_opus_model
    }
}

$env:ANTHROPIC_API_KEY = $state.router_api_key
$env:ANTHROPIC_BASE_URL = $state.router_base_url
$env:ANTHROPIC_DEFAULT_SONNET_MODEL = $sonnetAlias
$env:ANTHROPIC_DEFAULT_OPUS_MODEL = $opusAlias

$claudeArgs = @()
if ($Print) { $claudeArgs += "-p" }
if ($Bare) { $claudeArgs += "--bare" }
$claudeArgs += @("--model", $Model)
if ($Prompt) { $claudeArgs += $Prompt }

Write-Output ("Using 9router base URL: {0}" -f $state.router_base_url)
Write-Output ("Using mode={0}" -f $Mode)
Write-Output ("Using model alias: sonnet={0}, opus={1}" -f $sonnetAlias, $opusAlias)
Write-Output ("Running: claude {0}" -f ($claudeArgs -join " "))

& claude @claudeArgs
