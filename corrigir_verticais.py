with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print("Corrigindo linhas verticais para alinhar o │ final:")
print("=" * 70)

for i in range(68, 85):  # linhas 69-85
    if '│' in lines[i]:
        parts = lines[i].split('│')
        if len(parts) >= 2:
            # Pegar o conteúdo entre os │
            content = parts[1]
            # Remover espaços extras no final
            content_stripped = content.rstrip()
            # Adicionar espaços para completar exatamente 55 caracteres
            if len(content_stripped) < 55:
                content_fixed = content_stripped + ' ' * (55 - len(content_stripped))
            else:
                content_fixed = content_stripped[:55]
            
            # Reconstruir a linha
            lines[i] = f'│{content_fixed}│\n'
            print(f'Linha {i+1}: Corrigido - removidos espaços extras no final')

with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("\n✅ Correção aplicada! Todas as linhas verticais agora têm o │ final alinhado.")
