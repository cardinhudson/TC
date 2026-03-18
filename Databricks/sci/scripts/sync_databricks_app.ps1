param(
    [switch]$Watch,
    [string]$WorkspacePath = "/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app",
    [string]$LocalPath = "",
    [string]$Profile = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($LocalPath)) {
    $LocalPath = Join-Path $PSScriptRoot "..\Databricks\sci_app"
}

$LocalPath = [System.IO.Path]::GetFullPath($LocalPath)

if (-not (Test-Path $LocalPath)) {
    throw "Pasta local nao encontrada: $LocalPath"
}

$databricksCmd = Get-Command databricks -ErrorAction SilentlyContinue
if (-not $databricksCmd) {
    Write-Host "Databricks CLI nao encontrado no PATH." -ForegroundColor Yellow
    Write-Host "Instale o CLI e execute novamente este script." -ForegroundColor Yellow
    Write-Host "Comando esperado:" -ForegroundColor Cyan
    Write-Host "  databricks sync --watch `"$LocalPath`" `"$WorkspacePath`"" -ForegroundColor White
    exit 1
}

$args = @()
if (-not [string]::IsNullOrWhiteSpace($Profile)) {
    $args += "--profile"
    $args += $Profile
}

$args += "sync"
if ($Watch) {
    $args += "--watch"
}
$args += $LocalPath
$args += $WorkspacePath

Write-Host "Sincronizando app Databricks" -ForegroundColor Cyan
Write-Host "Local: $LocalPath" -ForegroundColor Gray
Write-Host "Workspace: $WorkspacePath" -ForegroundColor Gray
if ($Watch) {
    Write-Host "Modo: watch" -ForegroundColor Gray
}

& $databricksCmd.Source @args
