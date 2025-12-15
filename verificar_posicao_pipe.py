with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print("Posição do último │ em cada linha:")
print("=" * 70)

positions = []
for i in range(68, 85):  # linhas 69-85
    if '│' in lines[i]:
        # Encontrar a posição do último │
        last_pipe_pos = lines[i].rfind('│')
        positions.append((i+1, last_pipe_pos, len(lines[i].rstrip())))
        print(f'Linha {i+1:2d}: │ final na posição {last_pipe_pos:2d} | Total linha: {len(lines[i].rstrip())} chars')

# Verificar se todas as posições são iguais
unique_positions = set(pos for _, pos, _ in positions)
if len(unique_positions) == 1:
    print(f"\n✅ Todas as linhas têm o │ final na mesma posição: {unique_positions.pop()}")
else:
    print(f"\n✗ PROBLEMA: As linhas têm o │ final em posições diferentes: {unique_positions}")
    print("   Isso causa o desalinhamento visual!")
