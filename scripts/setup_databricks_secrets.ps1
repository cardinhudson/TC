<#
.SYNOPSIS
    Provisiona os secrets no Databricks Secret Scope "sci".

.DESCRIPTION
    Usa a REST API do Databricks para:
    1. Criar o scope "sci" (ignora se ja existe)
    2. Popular DATABRICKS_TOKEN com o PAT do .env
    3. Popular SCI_PIPELINE_CLUSTER_ID (se fornecido)
    4. Conceder ACL READ ao Service Principal do App

    Requer: DATABRICKS_HOST e DATABRICKS_TOKEN no .env local.

.PARAMETER ClusterId
    (Opcional) ID do cluster para SCI_PIPELINE_CLUSTER_ID.

.PARAMETER AppSpId
    (Opcional) Application ID (client_id) do Service Principal do App.
    Se fornecido, concede permissao READ no scope.
#>
param(
    [string]$ClusterId = "",
    [string]$AppSpId = "d0c600b4-761d-455a-a3aa-937bf71bd49f"
)

$ErrorActionPreference = "Stop"

# --- Carregar .env ---
$envFile = Join-Path $PSScriptRoot "..\\.env"
if (-not (Test-Path $envFile)) {
    Write-Error "Arquivo .env nao encontrado em: $envFile"
    exit 1
}

$envVars = @{}
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
        $parts = $line -split "=", 2
        if ($parts.Count -eq 2) {
            $envVars[$parts[0].Trim()] = $parts[1].Trim()
        }
    }
}

$host_url = $envVars["DATABRICKS_HOST"]
$token    = $envVars["DATABRICKS_TOKEN"]

if (-not $host_url -or -not $token) {
    Write-Error "DATABRICKS_HOST e DATABRICKS_TOKEN devem estar definidos no .env"
    exit 1
}

$host_url = $host_url.TrimEnd("/")
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}

Write-Host "=== Provisionamento de Secrets Databricks ===" -ForegroundColor Cyan
Write-Host "Host: $host_url"
Write-Host ""

# --- 1. Criar scope "sci" ---
Write-Host "[1/3] Criando scope 'sci'..." -ForegroundColor Yellow
try {
    $body = @{ scope = "sci"; initial_manage_principal = "users" } | ConvertTo-Json
    Invoke-RestMethod -Uri "$host_url/api/2.0/secrets/scopes/create" `
        -Method POST -Headers $headers -Body $body -ContentType "application/json"
    Write-Host "  OK: Scope 'sci' criado." -ForegroundColor Green
} catch {
    $err = $_.ErrorDetails.Message
    if ($err -and $err -match "RESOURCE_ALREADY_EXISTS") {
        Write-Host "  OK: Scope 'sci' ja existe." -ForegroundColor Green
    } else {
        Write-Host "  WARN: $($_.Exception.Message)" -ForegroundColor DarkYellow
        # Tentar continuar mesmo assim
    }
}

# --- 2. Popular DATABRICKS_TOKEN ---
Write-Host "[2/3] Salvando secret DATABRICKS_TOKEN..." -ForegroundColor Yellow
try {
    $body = @{
        scope        = "sci"
        key          = "DATABRICKS_TOKEN"
        string_value = $token
    } | ConvertTo-Json
    Invoke-RestMethod -Uri "$host_url/api/2.0/secrets/put" `
        -Method POST -Headers $headers -Body $body -ContentType "application/json"
    Write-Host "  OK: DATABRICKS_TOKEN salvo no scope 'sci'." -ForegroundColor Green
} catch {
    Write-Error "  ERRO ao salvar DATABRICKS_TOKEN: $($_.Exception.Message)"
}

# --- 3. Popular SCI_PIPELINE_CLUSTER_ID (se fornecido) ---
if ($ClusterId) {
    Write-Host "[3/3] Salvando secret SCI_PIPELINE_CLUSTER_ID..." -ForegroundColor Yellow
    try {
        $body = @{
            scope        = "sci"
            key          = "SCI_PIPELINE_CLUSTER_ID"
            string_value = $ClusterId
        } | ConvertTo-Json
        Invoke-RestMethod -Uri "$host_url/api/2.0/secrets/put" `
            -Method POST -Headers $headers -Body $body -ContentType "application/json"
        Write-Host "  OK: SCI_PIPELINE_CLUSTER_ID salvo." -ForegroundColor Green
    } catch {
        Write-Error "  ERRO ao salvar SCI_PIPELINE_CLUSTER_ID: $($_.Exception.Message)"
    }
} else {
    Write-Host "[3/3] SCI_PIPELINE_CLUSTER_ID: pulado (use -ClusterId para definir)" -ForegroundColor DarkGray
}

# --- 4. Conceder ACL ao Service Principal do App ---
if ($AppSpId) {
    Write-Host "[4/4] Concedendo ACL READ ao Service Principal $AppSpId..." -ForegroundColor Yellow
    try {
        $body = @{
            scope      = "sci"
            principal  = $AppSpId
            permission = "READ"
        } | ConvertTo-Json
        Invoke-RestMethod -Uri "$host_url/api/2.0/secrets/acls/put" `
            -Method POST -Headers $headers -Body $body -ContentType "application/json"
        Write-Host "  OK: ACL READ concedida ao SP $AppSpId." -ForegroundColor Green
    } catch {
        $err = $_.ErrorDetails.Message
        if ($err -and $err -match "PERMISSION_DENIED") {
            Write-Host "  WARN: Sem permissao para alterar ACLs (precisa de MANAGE no scope)." -ForegroundColor DarkYellow
        } else {
            Write-Host "  WARN: $($_.Exception.Message)" -ForegroundColor DarkYellow
        }
    }
} else {
    Write-Host "[4/4] ACL: pulado (use -AppSpId para definir)" -ForegroundColor DarkGray
}

# --- Verificar ---
Write-Host ""
Write-Host "=== Verificando secrets no scope 'sci' ===" -ForegroundColor Cyan
try {
    $secrets = Invoke-RestMethod -Uri "$host_url/api/2.0/secrets/list?scope=sci" `
        -Method GET -Headers $headers -ContentType "application/json"
    if ($secrets.secrets) {
        foreach ($s in $secrets.secrets) {
            Write-Host "  - $($s.key)  (atualizado: $($s.last_updated_timestamp))" -ForegroundColor Green
        }
    } else {
        Write-Host "  (nenhum secret encontrado)" -ForegroundColor Red
    }
} catch {
    Write-Host "  WARN: Nao foi possivel listar secrets: $($_.Exception.Message)" -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "=== Concluido ===" -ForegroundColor Cyan
Write-Host "Reinicie o Databricks App para que ele leia os novos secrets."
