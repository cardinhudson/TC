import ast
import sys
files = [
    'pages/1_Waterfall.py',
    'tc_principal/pages/waterfall_tc.py',
]
for f in files:
    try:
        ast.parse(open(f, encoding='utf-8').read())
        print(f'{f}: OK')
        sys.stdout.flush()
    except SyntaxError as e:
        print(f'{f}: ERRO - {e}')
        sys.stdout.flush()
print('DONE')
sys.stdout.flush()
