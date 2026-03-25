import re, os, collections

files = []
for root, dirs, fnames in os.walk('.'):
    dirs[:] = [d for d in dirs if d not in ('__pycache__', 'build', 'cache', '.venv', '.git', 'node_modules')]
    for f in fnames:
        if f.endswith('.py'):
            path = os.path.join(root, f)
            if any(p in path for p in ['pages', 'alertas']):
                files.append(path)

key_pattern = re.compile(r'''\bkey\s*=\s*(?:f?['"]([^'"]+)['"]|(\w+))''')

for fpath in sorted(files):
    try:
        with open(fpath, 'r', encoding='utf-8') as fh:
            lines = fh.readlines()
    except:
        continue
    
    keys_found = collections.defaultdict(list)
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith('#'):
            continue
        if 'key=lambda' in line or 'dataset_key' in line:
            continue
            
        matches = key_pattern.findall(line)
        for m in matches:
            key_val = m[0] or m[1]
            if key_val and '{' not in key_val:
                keys_found[key_val].append(i)
    
    dupes = {k: v for k, v in keys_found.items() if len(v) > 1}
    if dupes:
        print(f'\n=== {fpath} ===')
        for k, lines_list in dupes.items():
            print(f'  DUPLICATE key="{k}" at lines: {lines_list}')

print("\n=== DONE ===")
