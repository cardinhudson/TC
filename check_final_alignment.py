with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print("Verificação FINAL do alinhamento vertical:")
print("=" * 70)

positions = []
for i in range(68, 85):
    line = lines[i]
    if '│' in line:
        last_pos = line.rfind('│')
        positions.append(last_pos)
        first_pos = line.find('│')
        content = line[first_pos+1:last_pos]
        print(f'Linha {i+1:2d}: │ final pos {last_pos:2d} | {len(content):2d} chars entre │')

unique = set(positions)
print(f"\n{'='*70}")
if len(unique) == 1:
    print(f"✅ PERFEITO! Todas as linhas têm │ final na posição {unique.pop()}")
else:
    print(f"✗ PROBLEMA: {len(unique)} posições diferentes: {sorted(unique)}")
