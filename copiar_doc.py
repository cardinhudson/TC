#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

src = r'C:\Users\hudso\Downloads\5 - Documentacao.py'
dst = 'pages/6 - Documentacao.py'

os.makedirs('pages', exist_ok=True)

with open(src, 'r', encoding='utf-8') as f:
    content = f.read()

with open(dst, 'w', encoding='utf-8') as f:
    f.write(content)

print(f'✅ Arquivo copiado! {len(content)} caracteres, {len(content.splitlines())} linhas')


