"""Scan for remaining relative 'dados' paths in source .py files."""
import os, re

pattern = re.compile(r"""os\.path\.join\s*\(\s*['"]dados['"]""")
count = 0
for root, dirs, files in os.walk('.'):
    if 'dist' in root or '__pycache__' in root or '.venv' in root:
        continue
    for f in files:
        if f.endswith('.py') and f != '_check_paths.py':
            path = os.path.join(root, f)
            with open(path, encoding='utf-8', errors='ignore') as fh:
                for i, line in enumerate(fh, 1):
                    if pattern.search(line):
                        print(f'{path}:{i}: {line.strip()[:120]}')
                        count += 1

# Also check f-string style: f'dados/TC_...
pattern2 = re.compile(r"""f['"]dados/TC""")
for root, dirs, files in os.walk('.'):
    if 'dist' in root or '__pycache__' in root or '.venv' in root:
        continue
    for f in files:
        if f.endswith('.py') and f != '_check_paths.py':
            path = os.path.join(root, f)
            with open(path, encoding='utf-8', errors='ignore') as fh:
                for i, line in enumerate(fh, 1):
                    if pattern2.search(line):
                        print(f'{path}:{i}: {line.strip()[:120]}')
                        count += 1

print(f'\nTotal remaining: {count}')
