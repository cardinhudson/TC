# Script para fazer commit das alterações do Stellantis Cost Intelligence (SCI)
# Data: 15/01/2026 | Atualizado: 26/02/2026

Write-Host "🚀 Preparando commit das alterações..." -ForegroundColor Cyan
Write-Host ""

# Adicionar TODOS os arquivos do projeto (respeitando .gitignore)
Write-Host "📦 Adicionando arquivos ao commit..." -ForegroundColor Yellow

# Usa git add -A para capturar TUDO: raiz + subdiretorios
# .gitignore já exclui venv/, __pycache__/, build/, dist/, etc.
git add -A

Write-Host ""
Write-Host "📊 Status atual:" -ForegroundColor Yellow
git status --short

Write-Host ""
Write-Host "💡 Arquivos ignorados (venv, __pycache__, build, etc.) são excluídos automaticamente via .gitignore" -ForegroundColor Green
Write-Host ""

# Perguntar confirmação
$resposta = Read-Host "Deseja fazer o commit agora? (S/N)"

if ($resposta -eq "S" -or $resposta -eq "s") {
    Write-Host ""
    $mensagem = Read-Host "Digite a mensagem do commit"
    
    if ($mensagem) {
        git commit -m "$mensagem"
        Write-Host ""
        Write-Host "✅ Commit realizado com sucesso!" -ForegroundColor Green
        Write-Host ""
        
        $push = Read-Host "Deseja fazer push para o repositório remoto? (S/N)"
        if ($push -eq "S" -or $push -eq "s") {
            git push
            Write-Host ""
            Write-Host "✅ Push realizado com sucesso!" -ForegroundColor Green
        }
    } else {
        Write-Host "❌ Commit cancelado - mensagem vazia" -ForegroundColor Red
    }
} else {
    Write-Host ""
    Write-Host "ℹ️ Commit cancelado pelo usuário" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Para fazer o commit manualmente, use:" -ForegroundColor Cyan
    Write-Host "  git commit -m 'Sua mensagem aqui'" -ForegroundColor White
    Write-Host "  git push" -ForegroundColor White
}

Write-Host ""
Write-Host "✅ Script finalizado!" -ForegroundColor Green
