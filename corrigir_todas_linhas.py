with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Linhas da seção ANTES (índice 0-based: 67-84, que são linhas 68-85)
# Precisamos garantir que cada linha tenha exatamente 55 caracteres entre │

for i in range(67, 85):
    if '│' in lines[i]:
        parts = lines[i].split('│')
        if len(parts) >= 2:
            content = parts[1].rstrip('\n\r')
            current_len = len(content)
            
            if current_len < 55:
                # Adicionar espaços no final
                content = content + ' ' * (55 - current_len)
            elif current_len > 55:
                # Remover caracteres do final (preservar conteúdo importante)
                content = content[:55]
            
            # Reconstruir a linha
            lines[i] = f'│{content}│\n'
            print(f'Linha {i+1}: Corrigido de {current_len} para {len(content)} caracteres')

with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("\n✓ Todas as linhas corrigidas para 55 caracteres!")
