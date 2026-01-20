# Script PowerShell para ativar o ambiente virtual

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Ativando Ambiente Virtual Python" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Preferir .venv (padrão recomendado), com fallback para venv
$venvPath = ".venv"
if (-not (Test-Path "$venvPath\Scripts\Activate.ps1")) {
    if (Test-Path "venv\Scripts\Activate.ps1") {
        $venvPath = "venv"
    }
}

# Verificar se o ambiente virtual existe
if (-not (Test-Path "$venvPath\Scripts\Activate.ps1")) {
    Write-Host "[ERRO] Ambiente virtual não encontrado!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Criando ambiente virtual..." -ForegroundColor Yellow
    $venvPath = ".venv"
    python -m venv $venvPath
    Write-Host ""
    Write-Host "Instalando dependências..." -ForegroundColor Yellow
    & "$venvPath\Scripts\Activate.ps1"
    pip install -r requirements.txt
    Write-Host ""
    Write-Host "Ambiente virtual criado e dependências instaladas!" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Ativando ambiente virtual..." -ForegroundColor Yellow
    & "$venvPath\Scripts\Activate.ps1"
    Write-Host ""
    Write-Host "Ambiente virtual ativado!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Para executar a aplicação, use:" -ForegroundColor Cyan
    Write-Host "  streamlit run app.py" -ForegroundColor White
    Write-Host ""
}


