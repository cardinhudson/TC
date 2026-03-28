param(
    [switch]$Watch,
    [switch]$DeployOnly,
    [switch]$SkipDeploy,
    [string]$WorkspacePath = "/Workspace/Users/u235107@inetpsa.com/Drafts/sci_app/sci_app",
    [string]$LocalPath = "",
    [string]$Profile = "",
    [string]$AppName = "sci",
    [string]$ExternalRepoPath = "C:\user\U235107\GitSTLA\TC-Cloud"
)

$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$PrimaryMirrorPath = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot "Databricks\sci_app"))
$SecondaryMirrorPath = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot "Databricks\sci"))

$PublishItems = @(
    ".streamlit",
    "alertas",
    "APRESENTACAO_5_MINUTOS_VISUAL.md",
    "app.py",
    "app.yaml",
    "chatbot_documentacao.py",
    "controle_paginas.json",
    "dados_equipe.json",
    "DOCUMENTACAO_SISTEMA_TC.md",
    "DOCUMENTACAO_TC_PRINCIPAL.md",
    "DOCUMENTACAO_TC_CLOUD.md",
    "GUIA_DATABRICKS_100_NUVEM.md",
    "notebooks",
    "pages",
    "processamento_dados.py",
    "processamento_dados_BUD.py",
    "processamento_dados_veiculos.py",
    "processamento_dados_veiculos_BUD.py",
    "rateios_manuais.json",
    "README_cloud.md",
    "README_DATABRICKS.md",
    "requirements.txt",
    "SCI_faixa.png",
    "scripts",
    "src",
    "tc_copilot",
    "tc_core",
    "tc_exports.py",
    "tc_ext",
    "tc_principal",
    "versao.json",
    "versionamento.py"
)

function Invoke-RobocopySync {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination
    )

    New-Item -ItemType Directory -Force -Path $Destination | Out-Null

    & robocopy $Source $Destination /MIR /FFT /R:1 /W:1 /NFL /NDL /NJH /NJS /NP /XD ".git" "__pycache__" ".pytest_cache" | Out-Null
    if ($LASTEXITCODE -ge 8) {
        throw "Falha ao sincronizar pasta via robocopy: $Source -> $Destination (codigo $LASTEXITCODE)"
    }
}

function Sync-FileWithParent {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination
    )

    $destinationDir = Split-Path -Parent $Destination
    if (-not [string]::IsNullOrWhiteSpace($destinationDir)) {
        New-Item -ItemType Directory -Force -Path $destinationDir | Out-Null
    }

    Copy-Item -Path $Source -Destination $Destination -Force
}

function Sync-Item {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination
    )

    if (-not (Test-Path $Source)) {
        return
    }

    $sourceItem = Get-Item $Source
    if ($sourceItem.PSIsContainer) {
        Invoke-RobocopySync -Source $sourceItem.FullName -Destination $Destination
        return
    }

    Sync-FileWithParent -Source $sourceItem.FullName -Destination $Destination
}

function Sync-AppMirror {
    param(
        [Parameter(Mandatory = $true)][string]$MirrorRoot
    )

    if (-not (Test-Path $MirrorRoot)) {
        Write-Host "Espelho nao encontrado, pulando: $MirrorRoot" -ForegroundColor Yellow
        return
    }

    Write-Host "Atualizando espelho local: $MirrorRoot" -ForegroundColor Cyan
    foreach ($item in $PublishItems) {
        $source = Join-Path $RepoRoot $item
        $destination = Join-Path $MirrorRoot $item
        Sync-Item -Source $source -Destination $destination
    }

    $rootScriptPath = Join-Path $RepoRoot "scripts\sync_databricks_app.ps1"
    $mirrorScriptPath = Join-Path $MirrorRoot "scripts\sync_databricks_app.ps1"
    Sync-FileWithParent -Source $rootScriptPath -Destination $mirrorScriptPath
}

function Sync-ExternalRepo {
    param(
        [Parameter(Mandatory = $true)][string]$SourceMirrorRoot,
        [string]$TargetRepoRoot
    )

    if ([string]::IsNullOrWhiteSpace($TargetRepoRoot)) {
        return
    }

    $targetRepoRoot = [System.IO.Path]::GetFullPath($TargetRepoRoot)
    if (-not (Test-Path $targetRepoRoot)) {
        Write-Host "Repositorio externo nao encontrado, pulando: $targetRepoRoot" -ForegroundColor Yellow
        return
    }

    $targetAppRoot = Join-Path $targetRepoRoot "sci_app"
    $targetScriptPath = Join-Path $targetRepoRoot "scripts\sync_databricks_app.ps1"

    Write-Host "Atualizando repositorio externo: $targetRepoRoot" -ForegroundColor Cyan
    Invoke-RobocopySync -Source $SourceMirrorRoot -Destination $targetAppRoot
    Sync-FileWithParent -Source (Join-Path $RepoRoot "scripts\sync_databricks_app.ps1") -Destination $targetScriptPath
}

function Get-DatabricksCliCommand {
    $cmd = Get-Command databricks -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    $wingetRoot = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages"
    if (Test-Path $wingetRoot) {
        $candidate = Get-ChildItem -Path $wingetRoot -Directory -Filter "Databricks.DatabricksCLI_*" -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if ($candidate) {
            $exePath = Join-Path $candidate.FullName "databricks.exe"
            if (Test-Path $exePath) {
                return $exePath
            }
        }
    }

    $aliasPath = Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps\databricks.exe"
    if (Test-Path $aliasPath) {
        return $aliasPath
    }

    return $null
}

function Import-LocalEnvFile {
    param(
        [Parameter(Mandatory = $true)][string]$EnvFilePath
    )

    if (-not (Test-Path $EnvFilePath)) {
        return
    }

    Get-Content $EnvFilePath | ForEach-Object {
        if ($_ -match '^(?!#)\s*([^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            if (-not [string]::IsNullOrWhiteSpace($name)) {
                [System.Environment]::SetEnvironmentVariable($name, $value, 'Process')
            }
        }
    }
}

if ([string]::IsNullOrWhiteSpace($LocalPath)) {
    $LocalPath = $PrimaryMirrorPath
}

$LocalPath = [System.IO.Path]::GetFullPath($LocalPath)

Import-LocalEnvFile -EnvFilePath (Join-Path $RepoRoot ".env")

if ([string]::IsNullOrWhiteSpace($Profile)) {
    if (-not [string]::IsNullOrWhiteSpace($env:SCI_DATABRICKS_PROFILE)) {
        $Profile = $env:SCI_DATABRICKS_PROFILE
    } elseif (-not [string]::IsNullOrWhiteSpace($env:DATABRICKS_CONFIG_PROFILE)) {
        $Profile = $env:DATABRICKS_CONFIG_PROFILE
    }
}

if (-not (Test-Path $LocalPath)) {
    throw "Pasta local nao encontrada: $LocalPath"
}

Sync-AppMirror -MirrorRoot $PrimaryMirrorPath
Sync-AppMirror -MirrorRoot $SecondaryMirrorPath
Sync-ExternalRepo -SourceMirrorRoot $PrimaryMirrorPath -TargetRepoRoot $ExternalRepoPath

$databricksCmd = Get-DatabricksCliCommand
if (-not $databricksCmd) {
    Write-Host "Espelhos locais atualizados com sucesso." -ForegroundColor Green
    Write-Host "Databricks CLI nao encontrado no PATH." -ForegroundColor Yellow
    Write-Host "Instale o CLI e execute novamente este script." -ForegroundColor Yellow
    Write-Host "Comando esperado:" -ForegroundColor Cyan
    Write-Host "  databricks sync --watch `"$LocalPath`" `"$WorkspacePath`"" -ForegroundColor White
    exit 0
}

$syncArgs = @()
if (-not [string]::IsNullOrWhiteSpace($Profile)) {
    $syncArgs += "--profile"
    $syncArgs += $Profile
}

$importArgs = @()
if (-not [string]::IsNullOrWhiteSpace($Profile)) {
    $importArgs += "--profile"
    $importArgs += $Profile
}

$authArgs = @($importArgs)

function Invoke-WorkspaceImportTree {
    param(
        [string]$LocalRoot,
        [string]$RemoteRoot,
        [string]$DatabricksExe,
        [string[]]$DatabricksArgs
    )

    $createdFolders = @{}

    $allFiles = Get-ChildItem -Path $LocalRoot -Recurse -File |
        Where-Object {
            $_.FullName -notlike "*\__pycache__\*" -and
            $_.FullName -notlike "*\.git\*" -and
            $_.Name -ne ".gitignore"
        }

    $total = $allFiles.Count
    $ok = 0
    $fail = 0
    $failedFiles = @()
    $maxRetries = 3

    foreach ($file in $allFiles) {
        $relPath = $file.FullName.Substring($LocalRoot.TrimEnd('\').Length + 1) -replace '\\','/'
        $remotePath = "$RemoteRoot/$relPath"
        $remoteDir = (Split-Path -Path $remotePath -Parent) -replace '\\','/'
        $uploaded = $false
        for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
            try {
                if (-not [string]::IsNullOrWhiteSpace($remoteDir) -and -not $createdFolders.ContainsKey($remoteDir)) {
                    $mkdirOutput = & $DatabricksExe @DatabricksArgs workspace mkdirs $remoteDir 2>&1
                    if ($LASTEXITCODE -ne 0) {
                        throw "Falha ao criar pasta remota '$remoteDir': $mkdirOutput"
                    }
                    $createdFolders[$remoteDir] = $true
                }
                $output = & $DatabricksExe @DatabricksArgs workspace import $remotePath --file $file.FullName --format AUTO --overwrite 2>&1
                if ($LASTEXITCODE -ne 0) {
                    throw "Exit code $LASTEXITCODE : $output"
                }
                $ok++
                $uploaded = $true
                break
            } catch {
                if ($attempt -lt $maxRetries) {
                    $wait = $attempt * 5
                    Write-Host "  RETRY ($attempt/$maxRetries): $relPath - aguardando ${wait}s..." -ForegroundColor Yellow
                    Start-Sleep -Seconds $wait
                } else {
                    Write-Host "  FALHA: $relPath - $($_.Exception.Message)" -ForegroundColor Red
                    $failedFiles += $relPath
                    $fail++
                }
            }
        }
    }
    Write-Host "Upload direto: $ok OK, $fail falhas (de $total arquivos)" -ForegroundColor $(if ($fail -eq 0) { "Green" } else { "Yellow" })
    if ($failedFiles.Count -gt 0) {
        Write-Host "Arquivos com falha:" -ForegroundColor Red
        $failedFiles | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    }
}

function Test-DatabricksAuth {
    param(
        [Parameter(Mandatory = $true)][string]$DatabricksExe,
        [string[]]$DatabricksArgs
    )

    $authOutput = & $DatabricksExe @DatabricksArgs current-user me 2>&1
    if ($LASTEXITCODE -ne 0) {
        $profileMsg = if ([string]::IsNullOrWhiteSpace($Profile)) {
            "Sem profile explicito. Configure DATABRICKS_CONFIG_PROFILE/SCI_DATABRICKS_PROFILE ou use -Profile."
        } else {
            "Profile atual: '$Profile'. Verifique se ele existe e esta autenticado no .databrickscfg."
        }
        throw "Databricks CLI sem autenticacao valida. $profileMsg`nSaida do CLI: $authOutput"
    }
}

function Invoke-DatabricksAppDeploy {
    param(
        [Parameter(Mandatory = $true)][string]$DatabricksExe,
        [string[]]$DatabricksArgs,
        [Parameter(Mandatory = $true)][string]$TargetAppName,
        [Parameter(Mandatory = $true)][string]$TargetWorkspacePath
    )

    Write-Host "" -ForegroundColor Cyan
    Write-Host "Executando deploy SNAPSHOT do app '$TargetAppName' e aguardando conclusao..." -ForegroundColor Cyan

    try {
        $deployOutput = & $DatabricksExe @DatabricksArgs apps deploy $TargetAppName --mode SNAPSHOT --source-code-path $TargetWorkspacePath --timeout 30m 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERRO: Deploy retornou codigo $LASTEXITCODE" -ForegroundColor Red
            Write-Host $deployOutput -ForegroundColor Yellow
            exit 1
        }

        Write-Host "Deploy SNAPSHOT concluido com sucesso." -ForegroundColor Green
        $deployOutput | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    } catch {
        throw "Falha ao disparar deploy do app '$TargetAppName': $($_.Exception.Message)"
    }
}

if ($Watch -and $DeployOnly) {
    throw "Use Watch ou DeployOnly, nao ambos ao mesmo tempo."
}

if ($DeployOnly) {
    Test-DatabricksAuth -DatabricksExe $databricksCmd -DatabricksArgs $authArgs

    Write-Host "Executando somente o deploy do app Databricks" -ForegroundColor Cyan
    Write-Host "Workspace: $WorkspacePath" -ForegroundColor Gray
    Write-Host "App: $AppName" -ForegroundColor Gray
    if (-not [string]::IsNullOrWhiteSpace($Profile)) {
        Write-Host "Profile: $Profile" -ForegroundColor Gray
    }

    Invoke-DatabricksAppDeploy -DatabricksExe $databricksCmd -DatabricksArgs $importArgs -TargetAppName $AppName -TargetWorkspacePath $WorkspacePath
} elseif ($Watch) {
    $syncArgs += "sync"
    $syncArgs += "--watch"
    $syncArgs += $LocalPath
    $syncArgs += $WorkspacePath

    Test-DatabricksAuth -DatabricksExe $databricksCmd -DatabricksArgs $authArgs

    Write-Host "Sincronizando app Databricks (modo watch)" -ForegroundColor Cyan
    Write-Host "Local: $LocalPath" -ForegroundColor Gray
    Write-Host "Workspace: $WorkspacePath" -ForegroundColor Gray
    if (-not [string]::IsNullOrWhiteSpace($Profile)) {
        Write-Host "Profile: $Profile" -ForegroundColor Gray
    }
    & $databricksCmd @syncArgs
} else {
    Test-DatabricksAuth -DatabricksExe $databricksCmd -DatabricksArgs $authArgs

    Write-Host "Sincronizando app Databricks (upload direto)" -ForegroundColor Cyan
    Write-Host "Local: $LocalPath" -ForegroundColor Gray
    Write-Host "Workspace: $WorkspacePath" -ForegroundColor Gray
    if (-not [string]::IsNullOrWhiteSpace($Profile)) {
        Write-Host "Profile: $Profile" -ForegroundColor Gray
    }
    Invoke-WorkspaceImportTree -LocalRoot $LocalPath -RemoteRoot $WorkspacePath -DatabricksExe $databricksCmd -DatabricksArgs $importArgs

    if ($SkipDeploy) {
        Write-Host "" -ForegroundColor Cyan
        Write-Host "Deploy pulado por parametro (-SkipDeploy)." -ForegroundColor Yellow
    } else {
        Invoke-DatabricksAppDeploy -DatabricksExe $databricksCmd -DatabricksArgs $importArgs -TargetAppName $AppName -TargetWorkspacePath $WorkspacePath
    }
}

Write-Host "" -ForegroundColor Cyan
Write-Host ("Espelhos atualizados: {0} | {1} | {2}" -f $PrimaryMirrorPath, $SecondaryMirrorPath, $ExternalRepoPath) -ForegroundColor Gray
