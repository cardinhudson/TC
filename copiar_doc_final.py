#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys

src = r'C:\Users\hudso\Downloads\5 - Documentacao.py'
dst = 'pages/6 - Documentacao.py'

try:
    # Criar diretório se não existir
    os.makedirs('pages', exist_ok=True)
    
    # Ler arquivo completo
    print(f"Lendo arquivo: {src}")
    with open(src, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"Conteúdo lido: {len(content)} caracteres, {len(content.splitlines())} linhas")
    
    # Escrever arquivo
    print(f"Escrevendo arquivo: {dst}")
    with open(dst, 'w', encoding='utf-8') as f:
        f.write(content)
    
    # Verificar se foi criado
    if os.path.exists(dst):
        size = os.path.getsize(dst)
        print(f"✅ Arquivo criado com sucesso! Tamanho: {size} bytes")
    else:
        print("❌ Erro: Arquivo não foi criado")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)


