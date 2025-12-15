#!/usr/bin/env python3
# -*- coding: utf-8 -*-

with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
i = 0
removed_count = 0

while i < len(lines):
    line = lines[i]
    
    # Verificar se é início de uma caixa ASCII (linha com ┌─)
    if '┌' in line and '─' in line and '┐' in line:
        # Encontrar o final da caixa (linha com └─)
        start_box = i
        end_box = i
        
        # Procurar a linha de fechamento
        for j in range(i + 1, len(lines)):
            if '└' in lines[j] and '─' in lines[j] and '┘' in lines[j]:
                end_box = j
                break
        
        # Extrair apenas o conteúdo interno (remover bordas)
        content_lines = []
        for j in range(start_box + 1, end_box):
            content_line = lines[j]
            # Remover │ do início e fim, mantendo apenas o conteúdo
            if '│' in content_line:
                first_pipe = content_line.find('│')
                last_pipe = content_line.rfind('│')
                if first_pipe < last_pipe:
                    # Extrair conteúdo entre os │
                    content = content_line[first_pipe+1:last_pipe].strip()
                    if content:  # Só adicionar se houver conteúdo
                        content_lines.append(content + '\n')
                    else:
                        content_lines.append('\n')  # Linha vazia
                else:
                    content_lines.append(content_line)
            else:
                content_lines.append(content_line)
        
        # Adicionar o conteúdo sem as bordas
        new_lines.extend(content_lines)
        removed_count += 1
        i = end_box + 1
    else:
        new_lines.append(line)
        i += 1

# Salvar arquivo
with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"✅ Removidas as bordas de {removed_count} caixas ASCII!")
print("   Todas as linhas de borda (┌, ┐, └, ┘, │) foram removidas.")
