with open('APRESENTACAO_5_MINUTOS_VISUAL.md', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print("Verificação final - Seção ANTES:")
print("=" * 60)
all_ok = True
for i in range(67, 85):
    if '│' in lines[i]:
        parts = lines[i].split('│')
        if len(parts) >= 2:
            content = parts[1]
            length = len(content)
            if length != 55:
                all_ok = False
                print(f'✗ Linha {i+1}: {length} caracteres (deveria ser 55)')
            else:
                print(f'✓ Linha {i+1}: {length} caracteres')

if all_ok:
    print("\n✅ TODAS AS LINHAS ESTÃO ALINHADAS CORRETAMENTE!")
