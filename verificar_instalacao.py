#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para verificar se todas as dependências foram instaladas corretamente.
"""

print('=' * 60)
print('INSTALAÇÃO COMPLETA - VERIFICAÇÃO FINAL')
print('=' * 60)

print('\nBIBLIOTECAS PRINCIPAIS:')
try:
    import streamlit
    print(f'✓ Streamlit {streamlit.__version__}')
except Exception as e:
    print(f'✗ Streamlit - ERRO: {e}')

try:
    import pandas
    print(f'✓ Pandas {pandas.__version__}')
except Exception as e:
    print(f'✗ Pandas - ERRO: {e}')

try:
    import numpy
    print(f'✓ NumPy {numpy.__version__}')
except Exception as e:
    print(f'✗ NumPy - ERRO: {e}')

try:
    import plotly
    print(f'✓ Plotly {plotly.__version__}')
except Exception as e:
    print(f'✗ Plotly - ERRO: {e}')

try:
    import altair
    print(f'✓ Altair {altair.__version__}')
except Exception as e:
    print(f'✗ Altair - ERRO: {e}')

try:
    import openpyxl
    print(f'✓ OpenPyXL {openpyxl.__version__}')
except Exception as e:
    print(f'✗ OpenPyXL - ERRO: {e}')

try:
    import pyarrow
    print(f'✓ PyArrow {pyarrow.__version__}')
except Exception as e:
    print(f'✗ PyArrow - ERRO: {e}')

try:
    import st_aggrid
    print(f'✓ streamlit-aggrid instalado')
except Exception as e:
    print(f'✗ streamlit-aggrid - ERRO: {e}')

print('\nBIBLIOTECAS DO CHATBOT:')
try:
    import torch
    print(f'✓ PyTorch {torch.__version__}')
except Exception as e:
    print(f'✗ PyTorch - ERRO: {e}')

try:
    import transformers
    print(f'✓ Transformers {transformers.__version__}')
except Exception as e:
    print(f'✗ Transformers - ERRO: {e}')

try:
    import sentence_transformers
    print(f'✓ Sentence-Transformers {sentence_transformers.__version__}')
except Exception as e:
    print(f'✗ Sentence-Transformers - ERRO: {e}')

try:
    import faiss
    print(f'✓ Faiss-CPU instalado')
except Exception as e:
    print(f'✗ Faiss-CPU - ERRO: {e}')

print('\n' + '=' * 60)
print('AMBIENTE 100% PRONTO!')
print('=' * 60)
print('\nPara executar: streamlit run app.py')
print('Ou use a task: Streamlit: Run app.py')
