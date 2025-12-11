# Script PowerShell para ativar o ambiente virtual

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Ativando Ambiente Virtual Python" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar se o ambiente virtual existe
if (-not (Test-Path "venv\Scripts\Activate.ps1")) {
    Write-Host "[ERRO] Ambiente virtual não encontrado!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Criando ambiente virtual..." -ForegroundColor Yellow
    python -m venv venv
    Write-Host ""
    Write-Host "Instalando dependências..." -ForegroundColor Yellow
    & "venv\Scripts\Activate.ps1"
    pip install -r requirements.txt
    Write-Host ""
    Write-Host "Ambiente virtual criado e dependências instaladas!" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Ativando ambiente virtual..." -ForegroundColor Yellow
    & "venv\Scripts\Activate.ps1"
    Write-Host ""
    Write-Host "Ambiente virtual ativado!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Para executar a aplicação, use:" -ForegroundColor Cyan
    Write-Host "  streamlit run app.py" -ForegroundColor White
    Write-Host ""
}

