#!/usr/bin/env python3
# -*- coding: utf-8 -*-

with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    content = f.read()

# Dividir em linhas
lines = content.split('\n')
new_lines = []
i = 0
in_code_block = False

while i < len(lines):
    line = lines[i]
    
    # Detectar início/fim de blocos de código
    if line.strip().startswith('```'):
        in_code_block = not in_code_block
        new_lines.append(line)
        i += 1
        continue
    
    # Se estamos dentro de um bloco de código
    if in_code_block:
        # Verificar se é uma linha de borda
        is_border_line = (
            ('┌' in line and '─' in line and '┐' in line) or  # Topo
            ('└' in line and '─' in line and '┘' in line) or  # Base
            (line.strip().startswith('│') and line.strip().endswith('│') and line.count('│') == 2)  # Lateral
        )
        
        if is_border_line:
            # Pular linha de borda
            i += 1
            continue
        else:
            # É uma linha de conteúdo - remover │ do início e fim se existirem
            if '│' in line:
                first_pipe = line.find('│')
                last_pipe = line.rfind('│')
                if first_pipe >= 0 and last_pipe > first_pipe:
                    # Extrair conteúdo entre os │
                    content = line[first_pipe+1:last_pipe].strip()
                    if content:
                        new_lines.append(content)
                    else:
                        new_lines.append('')  # Linha vazia
                    i += 1
                    continue
    
    # Linha normal - manter como está
    new_lines.append(line)
    i += 1

# Salvar
with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'w', encoding='utf-8') as f:
    f.write('\n'.join(new_lines))

print("✅ Todas as bordas ASCII foram removidas dos slides!")
