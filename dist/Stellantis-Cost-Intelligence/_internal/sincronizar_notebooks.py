"""
Script para sincronizar módulos Python com notebooks .ipynb
Este script verifica se os módulos processamento_dados.py e processamento_dados_BUD.py
estão atualizados com os notebooks tc_ext/notebooks/dados.ipynb e tc_ext/notebooks/dados_BUD.ipynb

Uso:
    python sincronizar_notebooks.py
"""

import json
import os
import re
from datetime import datetime
from typing import List, Dict, Tuple


def extrair_codigo_celulas(notebook_path: str) -> List[Dict[str, str]]:
    """
    Extrai o código de todas as células de um notebook Jupyter
    
    Returns:
        Lista de dicionários com 'cell_index' e 'source'
    """
    if not os.path.exists(notebook_path):
        print(f"⚠️ Notebook não encontrado: {notebook_path}")
        return []
    
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    celulas_codigo = []
    for idx, cell in enumerate(notebook.get('cells', [])):
        if cell.get('cell_type') == 'code':
            source = ''.join(cell.get('source', []))
            celulas_codigo.append({
                'cell_index': idx,
                'source': source,
                'execution_count': cell.get('execution_count')
            })
    
    return celulas_codigo


def verificar_funcionalidades_notebook(notebook_path: str, modulo_path: str) -> Tuple[List[str], List[str]]:
    """
    Verifica se todas as funcionalidades do notebook estão no módulo Python
    
    Returns:
        (funcionalidades_encontradas, funcionalidades_faltando)
    """
    celulas = extrair_codigo_celulas(notebook_path)
    
    if not os.path.exists(modulo_path):
        print(f"⚠️ Módulo não encontrado: {modulo_path}")
        return [], [f"Célula {i}" for i in range(len(celulas))]
    
    with open(modulo_path, 'r', encoding='utf-8') as f:
        modulo_content = f.read()
    
    funcionalidades_encontradas = []
    funcionalidades_faltando = []
    
    # Verificar funcionalidades principais
    funcionalidades_chave = [
        ('to_excel', 'Salvamento de arquivos Excel'),
        ('to_parquet', 'Salvamento de arquivos Parquet'),
        ('normalizar_tipos_para_parquet', 'Função de normalização'),
        ('consolidar_historico', 'Consolidação de histórico'),
        ('processar_completo', 'Função principal de processamento'),
        ('CAMINHO_DF_FINAL_XLSX', 'Caminho para Excel df_final'),
        ('CAMINHO_DF_VOL_XLSX', 'Caminho para Excel df_vol'),
        ('CAMINHO_DF_KE5Z_GROUP_XLSX', 'Caminho para Excel df_ke5z_group'),
        ('CAMINHO_DF_FINAL_CPU_XLSX', 'Caminho para Excel df_final_cpu'),
    ]
    
    for funcionalidade, descricao in funcionalidades_chave:
        if funcionalidade in modulo_content:
            funcionalidades_encontradas.append(descricao)
        else:
            funcionalidades_faltando.append(descricao)
    
    # Verificar células específicas
    celulas_importantes = {
        0: 'Configuração inicial',
        1: 'Leitura dados KE5Z/Sapiens',
        2: 'Merge com Base Conso',
        3: 'Processamento de Rateio',
        4: 'Merge KE5Z ↔ Rateio',
        5: 'Cálculo por veículo e salvamento Excel',
        6: 'Análise de percentuais',
        7: 'Somatória de colunas',
        8: 'Remoção de colunas percentual',
        9: 'Transformar veículos em linhas',
        10: 'Processamento de Volume',
        11: 'Filtro Account e salvamento Excel',
        12: 'Agrupamento e merge final',
        13: 'Salvamento e consolidação'
    }
    
    for cell_idx, descricao in celulas_importantes.items():
        if cell_idx < len(celulas):
            # Verificar se a funcionalidade está no módulo
            cell_source = celulas[cell_idx]['source']
            # Buscar palavras-chave da célula no módulo
            palavras_chave = extrair_palavras_chave(cell_source)
            encontrado = any(palavra in modulo_content for palavra in palavras_chave[:3])
            
            if encontrado:
                funcionalidades_encontradas.append(f"Célula {cell_idx}: {descricao}")
            else:
                funcionalidades_faltando.append(f"Célula {cell_idx}: {descricao}")
    
    return funcionalidades_encontradas, funcionalidades_faltando


def extrair_palavras_chave(codigo: str) -> List[str]:
    """Extrai palavras-chave únicas do código"""
    # Remover comentários e strings
    codigo_limpo = re.sub(r'#.*', '', codigo)
    codigo_limpo = re.sub(r'""".*?"""', '', codigo_limpo, flags=re.DOTALL)
    codigo_limpo = re.sub(r"'''.*?'''", '', codigo_limpo, flags=re.DOTALL)
    
    # Extrair palavras-chave (funções, variáveis importantes)
    palavras = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', codigo_limpo)
    palavras_unicas = list(set(palavras))
    
    # Filtrar palavras muito comuns
    palavras_comuns = {'pd', 'df', 'os', 'if', 'for', 'in', 'and', 'or', 'not', 'is', 'None', 'True', 'False'}
    palavras_chave = [p for p in palavras_unicas if p not in palavras_comuns and len(p) > 3]
    
    return palavras_chave[:10]  # Retornar top 10


def gerar_relatorio_sincronizacao():
    """Gera relatório de sincronização entre notebooks e módulos"""
    print("="*70)
    print("🔄 VERIFICAÇÃO DE SINCRONIZAÇÃO: Notebooks ↔ Módulos Python")
    print("="*70)
    print(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Verificar dados.ipynb ↔ processamento_dados.py
    print("📊 VERIFICANDO: tc_ext/notebooks/dados.ipynb ↔ processamento_dados.py")
    print("-"*70)
    encontradas_reais, faltando_reais = verificar_funcionalidades_notebook(
        os.path.join('tc_ext', 'notebooks', 'dados.ipynb'),
        'processamento_dados.py'
    )
    
    print(f"✅ Funcionalidades encontradas: {len(encontradas_reais)}")
    print(f"❌ Funcionalidades faltando: {len(faltando_reais)}")
    
    if faltando_reais:
        print("\n⚠️ Funcionalidades que podem estar faltando:")
        for func in faltando_reais[:10]:  # Mostrar apenas as primeiras 10
            print(f"   - {func}")
        if len(faltando_reais) > 10:
            print(f"   ... e mais {len(faltando_reais) - 10} funcionalidades")
    else:
        print("\n✅ Todos os módulos parecem estar sincronizados!")
    
    print("\n" + "="*70)
    
    # Verificar dados_BUD.ipynb ↔ processamento_dados_BUD.py
    print("💰 VERIFICANDO: tc_ext/notebooks/dados_BUD.ipynb ↔ processamento_dados_BUD.py")
    print("-"*70)
    encontradas_bud, faltando_bud = verificar_funcionalidades_notebook(
        os.path.join('tc_ext', 'notebooks', 'dados_BUD.ipynb'),
        'processamento_dados_BUD.py'
    )
    
    print(f"✅ Funcionalidades encontradas: {len(encontradas_bud)}")
    print(f"❌ Funcionalidades faltando: {len(faltando_bud)}")
    
    if faltando_bud:
        print("\n⚠️ Funcionalidades que podem estar faltando:")
        for func in faltando_bud[:10]:
            print(f"   - {func}")
        if len(faltando_bud) > 10:
            print(f"   ... e mais {len(faltando_bud) - 10} funcionalidades")
    else:
        print("\n✅ Todos os módulos parecem estar sincronizados!")
    
    print("\n" + "="*70)
    print("📋 RESUMO")
    print("="*70)
    print(f"Total de funcionalidades verificadas (REAIS): {len(encontradas_reais) + len(faltando_reais)}")
    print(f"  ✅ Encontradas: {len(encontradas_reais)}")
    print(f"  ❌ Faltando: {len(faltando_reais)}")
    print(f"\nTotal de funcionalidades verificadas (BUD): {len(encontradas_bud) + len(faltando_bud)}")
    print(f"  ✅ Encontradas: {len(encontradas_bud)}")
    print(f"  ❌ Faltando: {len(faltando_bud)}")
    
    if len(faltando_reais) == 0 and len(faltando_bud) == 0:
        print("\n🎉 Todos os módulos estão sincronizados com os notebooks!")
    else:
        print("\n⚠️ ATENÇÃO: Algumas funcionalidades podem estar faltando.")
        print("   Revise os módulos Python e compare com os notebooks originais.")
    
    print("="*70)


if __name__ == "__main__":
    gerar_relatorio_sincronizacao()

