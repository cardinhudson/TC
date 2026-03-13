"""
Módulo de Processamento de Dados REAL (Sapiens) — TC Veículos
Processa o arquivo 'Reporting veículos.xlsx' (aba Sapiens) para extrair custos
reais de produção de veículos.

Segue a mesma lógica metodológica do Budget (processamento_dados_veiculos_BUD.py),
com as seguintes diferenças:
  - Fonte de custo: aba 'Sapiens' (header=1) — dados já por linha/período
  - Redis: EXCLUÍDO do Sapiens; vem da aba 'massa - REDIS' (mesma fonte do Budget)
  - Fase 1B dedicada para massa-REDIS (Account real do Excel)
  - Volume/EST FA: aba 'Volume e EST PdR - Actual'
  - Tempo veículos: aba 'EST veículos - Actual'
  - Volume veículos: aba 'Volume Actual'
  - D&A Dedicado: reutiliza parquet gerado pelo Budget (df_dea_dedicado_BUD.parquet)
  - Saída: dados/TC_Principal/{ano}/ (raiz, sem subpasta BUD)

Fases:
   1. Sapiens             → Despesa Primaria (excluindo Redis)
   1B.massa - REDIS       → linhas receita (Account real do Excel)
   2. Volume e EST PdR    → Vol FA + Tempo FA (aba Actual)
   3. Volume Actual       → volumes de veículos
   4. EST veículos Actual → merge com volume → Tempo Veic
   5. Rateio FA           → %FA por oficina (automático BS/PS/PL, manual QY/GS/SM)
   6. Custo FA            → Rateio FA × Despesa Primaria
   7. Custo FP            → Despesa Primaria − Custo FA (fórmula unificada)
   8. D&A Dedicado        → carrega do Budget
   9. FP sem Dedicada     → Custo FP − D&A dedicado
  10. Salvamento          → parquets em dados/TC_Principal/{ano}/
  11. Custo FP sem D&A    → isolamento para rastreabilidade
  12. % Rateio veículos   → Tempo Veic / Total Tempo por (Oficina, Período)
  13. Custo rateado veíc. → FP sem Ded * Percentual
  14. Custo FP veículos   → rateado + D&A dedicado
  15. CPU veículos        → Custo FP Veiculo / Volume
  16. Salvamento veículos → parquets finais
  17. Comparativo R×B     → tabela comparativa Real vs Budget
  18. Validação final     → prova cruzada e integridade
"""

import sys as _sys
import pandas as pd
import numpy as np
import os
import json
import shutil
from datetime import datetime
from typing import Dict, Optional
import re
import unicodedata

from tc_core.utils.portabilidade import get_base_path, get_data_root

_ROOT = str(get_base_path())
_DATA_ROOT = os.path.join(str(get_data_root()), 'TC_Principal')


# ═══════════════════════════════════════════════════════════════
#  UTILITÁRIOS  (reutilizados de processamento_dados_veiculos_BUD)
# ═══════════════════════════════════════════════════════════════
from processamento_dados_veiculos_BUD import (
    MAPEAMENTO_MESES,
    PREFIXOS_MESES,
    OFICINAS_RATEIO_AUTOMATICO,
    OFICINAS_RATEIO_MANUAL,
    OFICINAS_EXCLUIR_DENOM_TAXA_PDR,
    _normalizar_nome_coluna,
    _corrigir_mojibake,
    _corrigir_colunas_mojibake,
    _detectar_colunas_meses,
    _normalizar_periodo,
    _exigir_colunas,
    _validar_abas_excel,
    normalizar_tipos_para_parquet,
)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTES DE FILTROS
# ═══════════════════════════════════════════════════════════════

# Oficinas que não devem ser processadas (não existem no Budget)
OFICINAS_INVALIDAS = ['Veículos', 'Projetos']


def filtrar_oficinas_validas(df: pd.DataFrame, contexto: str = "") -> pd.DataFrame:
    """
    Remove linhas de oficinas inválidas (Veículos, Projetos).
    
    Args:
        df: DataFrame com coluna 'Oficina'
        contexto: String descritiva para a mensagem de log
    
    Returns:
        DataFrame filtrado
    """
    if 'Oficina' not in df.columns:
        return df
    
    antes = len(df)
    mask_invalida = df['Oficina'].isin(OFICINAS_INVALIDAS)
    linhas_removidas = mask_invalida.sum()
    
    df_filtrado = df[~mask_invalida].copy()
    
    if linhas_removidas > 0:
        oficinas_encontradas = df[mask_invalida]['Oficina'].unique().tolist()
        print(f"   ℹ️ {linhas_removidas} linhas excluídas{contexto} (oficinas inválidas: {oficinas_encontradas})")
    
    return df_filtrado


# ═══════════════════════════════════════════════════════════════
#  ALIAS DE COLUNAS SAPIENS
# ═══════════════════════════════════════════════════════════════

ALIAS_COLUNAS_SAPIENS = {
    'Nºconta': ['Nºconta', 'N°conta', 'Nº conta', 'N° conta', 'No conta', 'Noconta'],
    'Período': ['Período', 'Periodo', 'Per\ufffdodo'],
    'Veículo': ['Veículo', 'Veiculo', 'Ve\ufffdculo'],
    'Oficina': ['Oficina'],
    'Account': ['Account'],
    'Valor': ['Valor'],
    'Type 05': ['Type 05', 'Type05'],
    'Type 06': ['Type 06', 'Type06'],
}


def _aplicar_alias_colunas(df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    """Aplica alias de colunas para lidar com variações de nome."""
    for nome_padrao, alternativas in alias_map.items():
        if nome_padrao not in df.columns:
            for alt in alternativas:
                if alt in df.columns:
                    df = df.rename(columns={alt: nome_padrao})
                    break
    return df


def _limpar_colunas_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas com sufixo numérico (.1, .2) geradas por duplicação."""
    cols_limpar = [c for c in df.columns if isinstance(c, str) and re.match(r'^.+\.\d+$', c)]
    if cols_limpar:
        df = df.drop(columns=cols_limpar, errors='ignore')
    return df


# ═══════════════════════════════════════════════════════════════
#  CONFIGURAÇÃO DO AMBIENTE
# ═══════════════════════════════════════════════════════════════

def configurar_ambiente(ano: Optional[int] = None) -> Dict:
    """
    Configura pastas e localiza o arquivo Excel.

    Cria a estrutura de diretórios necessária e valida a existência
    das abas obrigatórias no Excel.

    Retorna dict de configuração.
    """
    if ano is None:
        ano = datetime.now().year

    pasta_ano = os.path.join(_DATA_ROOT, str(ano))
    pasta_bud = os.path.join(pasta_ano, 'BUD')
    pasta_saida = pasta_ano  # Real salva na raiz do ano
    pasta_historico = os.path.join(_DATA_ROOT, 'historico_consolidado')

    os.makedirs(pasta_saida, exist_ok=True)
    os.makedirs(pasta_historico, exist_ok=True)

    # Localizar Excel
    caminho_excel = os.path.join(pasta_ano, 'Reporting veículos.xlsx')
    if not os.path.exists(caminho_excel):
        # Fallback: buscar na raiz
        caminho_raiz = os.path.join('.', 'Reporting veículos.xlsx')
        if os.path.exists(caminho_raiz):
            shutil.copy2(caminho_raiz, caminho_excel)
            print(f"   📋 Copiado da raiz → {caminho_excel}")
        else:
            raise FileNotFoundError(
                f"❌ Arquivo 'Reporting veículos.xlsx' não encontrado em:\n"
                f"   • {caminho_excel}\n"
                f"   • {caminho_raiz}"
            )

    # Validar abas obrigatórias para Real
    abas_obrigatorias = [
        'Sapiens',
        'massa - REDIS',
        'Volume e EST PdR - Actual',
        'Volume Actual',
        'EST veículos - Actual',
    ]
    _validar_abas_excel(caminho_excel, abas_obrigatorias)

    # Verificar se D&A do Budget existe
    caminho_dea_bud = os.path.join(pasta_bud, 'df_dea_dedicado_BUD.parquet')
    tem_dea_bud = os.path.exists(caminho_dea_bud)
    if not tem_dea_bud:
        print("   ⚠️ D&A Dedicado Budget não encontrado — FP sem Dedicada não será calculado")

    # Carregar rateios manuais
    rateios_path = 'rateios_manuais.json'
    if os.path.exists(rateios_path):
        with open(rateios_path, 'r', encoding='utf-8') as f:
            rateios = json.load(f)
    else:
        rateios = {'QY': 0.0, 'GS': 0.0, 'SM': 0.0}
        print(f"   ⚠️ rateios_manuais.json não encontrado — usando zeros")

    # ── Oficinas válidas (presentes no Budget) ──
    # Usado para filtrar o Sapiens e massa-REDIS, excluindo oficinas
    # como 'Projetos' que existem no Sapiens mas não no Budget/Excel.
    caminho_bud_principal = os.path.join(pasta_bud, 'df_principal_BUD.parquet')
    if os.path.exists(caminho_bud_principal):
        _df_bud = pd.read_parquet(caminho_bud_principal, columns=['Oficina'])
        oficinas_bud = sorted(_df_bud['Oficina'].dropna().unique().tolist())
        # Remover oficinas inválidas da lista do Budget
        oficinas_bud = [ofi for ofi in oficinas_bud if ofi not in OFICINAS_INVALIDAS]
        del _df_bud
    else:
        oficinas_bud = None  # sem filtro se BUD não processado
        print("   ⚠️ df_principal_BUD.parquet não encontrado — sem filtro de oficinas")

    config = {
        'ANO_ATUAL': ano,
        'PASTA_ANO': pasta_ano,
        'PASTA_BUD': pasta_bud,
        'PASTA_SAIDA': pasta_saida,
        'PASTA_HISTORICO': pasta_historico,
        'CAMINHO_EXCEL': caminho_excel,
        'CAMINHO_DEA_BUD': caminho_dea_bud,
        'TEM_DEA_BUD': tem_dea_bud,
        'RATEIOS_MANUAIS': rateios,
        'OFICINAS_BUD': oficinas_bud,
    }
    return config


# ═══════════════════════════════════════════════════════════════
#  FASE 1 — SAPIENS (custo real)
# ═══════════════════════════════════════════════════════════════

def fase1_sapiens(config: Dict) -> pd.DataFrame:
    """
    Lê aba 'Sapiens' (header=1) do Reporting veículos.xlsx.

    Os dados já estão por linha/período (diferente do BDG que tem meses como colunas).
    Coluna 'Valor' é renomeada para 'Despesa Primaria'.
    Linhas com Account='Redis' são EXCLUÍDAS (fonte correta: aba massa - REDIS).

    Retorna DataFrame com colunas:
      Oficina, Account, Type 05, Type 06, Custo, Período, Despesa Primaria, ...
    """
    print("\n📊 FASE 1 — Leitura aba Sapiens")

    caminho = config['CAMINHO_EXCEL']
    df = pd.read_excel(caminho, sheet_name='Sapiens', header=1)

    # Limpar colunas duplicadas e aplicar alias
    df = _limpar_colunas_duplicadas(df)
    df = _corrigir_colunas_mojibake(df)
    df = _aplicar_alias_colunas(df, ALIAS_COLUNAS_SAPIENS)

    # Colunas obrigatórias
    _exigir_colunas(df, ['Oficina', 'Account', 'Período', 'Valor'], "aba 'Sapiens'")

    # Renomear Valor → Despesa Primaria
    df = df.rename(columns={'Valor': 'Despesa Primaria'})

    # Conversões
    df['Despesa Primaria'] = pd.to_numeric(df['Despesa Primaria'], errors='coerce').fillna(0)

    # Normalizar Período
    df['Período'] = df['Período'].apply(_normalizar_periodo)

    # Remover linhas sem Oficina ou com DP = 0
    df = df[df['Oficina'].notna() & (df['Oficina'] != '')]
    df = df[df['Despesa Primaria'] != 0]

    # ═══ EXCLUIR linhas Redis do Sapiens ═══
    # Os valores de Redis vêm da aba 'massa - REDIS' (mesma fonte do Budget),
    # não do Sapiens. Excluir para evitar duplicação.
    n_redis_sapiens = (df['Account'] == 'Redis').sum()
    if n_redis_sapiens > 0:
        df = df[df['Account'] != 'Redis'].copy()
        print(f"   ℹ️ {n_redis_sapiens:,} linhas Redis excluídas do Sapiens (fonte correta: massa - REDIS)")

    # ═══ EXCLUIR oficinas inválidas (camada 1: lista hardcoded) ═══
    df = filtrar_oficinas_validas(df, "do Sapiens ")

    # ═══ EXCLUIR oficinas ausentes no Budget (camada 2: validação cruzada) ═══
    # Oficinas como 'Projetos' existem no Sapiens mas não no Budget/Excel.
    # Filtrar para manter apenas oficinas válidas (presentes no BUD).
    oficinas_bud = config.get('OFICINAS_BUD')
    if oficinas_bud is not None:
        mask_valida = df['Oficina'].isin(oficinas_bud)
        n_excluidas = (~mask_valida).sum()
        if n_excluidas > 0:
            oficinas_removidas = sorted(df.loc[~mask_valida, 'Oficina'].unique().tolist())
            df = df[mask_valida].copy()
            print(f"   ℹ️ {n_excluidas:,} linhas adicionais excluídas (não existem no BUD: {oficinas_removidas})")

    # Remover coluna Ano se existir (será adicionada no salvamento)
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # Garantir colunas estruturais
    for col in ['Type 05', 'Type 06', 'Custo']:
        if col not in df.columns:
            print(f"   ⚠️ Coluna '{col}' não encontrada na aba Sapiens — criando com valor padrão")
            df[col] = ''

    # Estatísticas
    n_total = len(df)
    print(f"   ✅ {n_total:,} linhas lidas (Redis excluído)")
    print(f"   Oficinas: {sorted(df['Oficina'].unique())}")
    print(f"   Períodos: {len(df['Período'].unique())}")
    print(f"   Despesa Primária total: R$ {df['Despesa Primaria'].sum():,.2f}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 1B — massa - REDIS (mesma fonte do Budget)
# ═══════════════════════════════════════════════════════════════

def fase1b_redis(config: Dict) -> pd.DataFrame:
    """
    Lê aba 'massa - REDIS' do Excel (mesma fonte do Budget).
    Preserva as colunas dimensionais do Excel (Account, Type 05, Type 06, Custo).
    Despesa Primaria é invertida (negativa, pois Redis é receita).

    Retorna DataFrame com mesma estrutura da fase1_sapiens.
    """
    print("\n📊 FASE 1B — massa - REDIS (receita)")

    caminho = config['CAMINHO_EXCEL']

    # Verificar se a aba existe
    try:
        df = pd.read_excel(caminho, sheet_name='massa - REDIS')
    except ValueError:
        print("   ⚠️ Aba 'massa - REDIS' não encontrada — pulando")
        return pd.DataFrame()

    df = _corrigir_colunas_mojibake(df)

    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        print("   ⚠️ Sem colunas de meses na aba 'massa - REDIS'")
        return pd.DataFrame()

    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='Despesa Primaria'
    )

    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)
    df_melt['Despesa Primaria'] = pd.to_numeric(df_melt['Despesa Primaria'], errors='coerce').fillna(0)

    # Inverter sinal: Redis é receita, deve ser negativo
    df_melt['Despesa Primaria'] = -df_melt['Despesa Primaria'].abs()

    # Remover linhas sem Oficina ou com DP = 0
    df_melt = df_melt[df_melt['Oficina'].notna() & (df_melt['Oficina'] != '')]
    df_melt = df_melt[df_melt['Despesa Primaria'] != 0]

    # NÃO sobrescrever — usar valores do Excel
    for col in ['Type 05', 'Type 06', 'Account', 'Custo']:
        if col not in df_melt.columns:
            print(f"   ⚠️ Coluna '{col}' ausente na aba massa - REDIS — criando vazia")
            df_melt[col] = ''

    # Marcar linhas como originadas da aba massa-REDIS (para KPI Redis na UI)
    df_melt['_fonte_redis'] = True

    # ═══ EXCLUIR oficinas inválidas (camada 1: lista hardcoded) ═══
    df_melt = filtrar_oficinas_validas(df_melt, "do Redis ")

    # ═══ EXCLUIR oficinas ausentes no Budget (camada 2: validação cruzada) ═══
    oficinas_bud = config.get('OFICINAS_BUD')
    if oficinas_bud is not None:
        mask_valida = df_melt['Oficina'].isin(oficinas_bud)
        n_excluidas = (~mask_valida).sum()
        if n_excluidas > 0:
            oficinas_removidas = sorted(df_melt.loc[~mask_valida, 'Oficina'].unique().tolist())
            df_melt = df_melt[mask_valida].copy()
            print(f"   ℹ️ {n_excluidas:,} linhas Redis adicionais excluídas (não existem no BUD: {oficinas_removidas})")

    # Agregar por chaves para evitar duplicatas
    chaves = ['Oficina', 'Período', 'Type 05', 'Type 06', 'Account', 'Custo', '_fonte_redis']
    colunas_agg = [c for c in chaves if c in df_melt.columns]
    df_melt = df_melt.groupby(colunas_agg, as_index=False)['Despesa Primaria'].sum()

    n_total = len(df_melt)
    accounts = sorted(df_melt['Account'].dropna().unique().tolist())
    print(f"   ✅ {n_total:,} linhas de massa-REDIS")
    print(f"   Accounts: {accounts}")
    print(f"   Despesa Primária total: R$ {df_melt['Despesa Primaria'].sum():,.2f}")

    return df_melt


# ═══════════════════════════════════════════════════════════════
#  FASE 2 — VOLUME E EST PdR (Actual)
# ═══════════════════════════════════════════════════════════════

def _ler_volume_est_fa_de_aba(caminho: str, sheet_name: str, label: str) -> pd.DataFrame:
    """
    Lê uma aba 'Volume e EST PdR' (Actual ou BDG), detectando header
    automaticamente e fazendo melt meses → linhas.

    Retorna DataFrame com colunas:
      REF FER, Oficina, EST, Período, Vol FA, Tempo FA
    """
    # Detectar header (pode variar de posição)
    df_raw = pd.read_excel(caminho, sheet_name=sheet_name, header=None, nrows=10)
    header_row = None
    for i in range(min(10, len(df_raw))):
        vals = [str(v).strip().lower() for v in df_raw.iloc[i].values if pd.notna(v)]
        if any('oficina' in v or 'ref' in v for v in vals):
            header_row = i
            break

    if header_row is None:
        raise ValueError(f"❌ Não encontrei cabeçalho na aba '{sheet_name}'")

    df = pd.read_excel(caminho, sheet_name=sheet_name, header=header_row)
    df = _corrigir_colunas_mojibake(df)

    # Renomear colunas
    for c in df.columns:
        cn = _normalizar_nome_coluna(c)
        if cn.startswith('ref'):
            df = df.rename(columns={c: 'REF FER'})
        elif cn == 'oficina':
            df = df.rename(columns={c: 'Oficina'})
        elif cn == 'est':
            df = df.rename(columns={c: 'EST'})

    _exigir_colunas(df, ['Oficina', 'EST'], f"aba '{sheet_name}'")

    # Remover Ano se existir
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # EST numérico
    df['EST'] = pd.to_numeric(df['EST'], errors='coerce').fillna(0)

    # Detectar colunas de meses
    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        raise ValueError(f"❌ Sem colunas de meses na aba '{sheet_name}'")

    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    # Melt
    df = df.melt(id_vars=colunas_dim, value_vars=colunas_meses,
                 var_name='Período', value_name='Vol FA')
    df['Período'] = df['Período'].apply(_normalizar_periodo)
    df['Vol FA'] = pd.to_numeric(df['Vol FA'], errors='coerce').fillna(0)

    # Calcular Tempo FA
    df['Tempo FA'] = df['Vol FA'] * df['EST']

    # Remover linhas sem Oficina
    df = df[df['Oficina'].notna() & (df['Oficina'] != '')]

    print(f"   ✅ [{label}] {len(df):,} linhas | ∑ Vol FA: {df['Vol FA'].sum():,.0f} | ∑ Tempo FA: {df['Tempo FA'].sum():,.2f}")
    print(f"   Oficinas: {sorted(df['Oficina'].unique())}")

    return df


def fase2_volume_est_fa(config: Dict) -> pd.DataFrame:
    """
    Lê aba 'Volume e EST PdR - Actual'.
    Se os dados Actual estiverem vazios (todos os meses = 0/NaN),
    faz FALLBACK automático para 'Volume e EST PdR - BDG'.

    Retorna DataFrame com colunas:
      REF FER, Oficina, EST, Período, Vol FA, Tempo FA
    """
    print("\n📊 FASE 2 — Volume e EST PdR (Actual)")

    caminho = config['CAMINHO_EXCEL']

    # 1. Tentar ler aba Actual
    df = _ler_volume_est_fa_de_aba(caminho, 'Volume e EST PdR - Actual', 'Actual')

    # 2. Verificar se há dados válidos (Vol FA > 0)
    vol_total = df['Vol FA'].sum()
    tempo_total = df['Tempo FA'].sum()

    if vol_total == 0 and tempo_total == 0:
        print("   ⚠️ Volume FA Actual VAZIO (todos os meses = 0)")
        print("   🔄 Aplicando FALLBACK: usando dados BDG para o rateio FA...")

        # Verificar se aba BDG existe
        try:
            df_bdg = _ler_volume_est_fa_de_aba(caminho, 'Volume e EST PdR - BDG', 'BDG Fallback')

            vol_bdg = df_bdg['Vol FA'].sum()
            if vol_bdg > 0:
                df = df_bdg
                df['_fonte_volume_fa'] = 'BDG'  # flag para rastreabilidade
                print(f"   ✅ Fallback BDG aplicado: ∑ Vol FA = {vol_bdg:,.0f}")
            else:
                print("   ❌ Aba BDG também está vazia — Rateio FA será 0")
                df['_fonte_volume_fa'] = 'Actual (vazio)'
        except Exception as e:
            print(f"   ❌ Falha ao ler BDG fallback: {e}")
            df['_fonte_volume_fa'] = 'Actual (vazio)'
    else:
        df['_fonte_volume_fa'] = 'Actual'
        print(f"   ✅ Dados Actual válidos: ∑ Vol FA = {vol_total:,.0f}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 3 — VOLUMES VEÍCULOS (Actual)
# ═══════════════════════════════════════════════════════════════

def fase3_volume_veiculos(config: Dict) -> pd.DataFrame:
    """
    Lê aba 'Volume Actual' → volumes de produção por veículo.

    Retorna DataFrame com colunas:
      Veículo, Período, Volume
    """
    print("\n📊 FASE 3 — Volume veículos (Actual)")

    caminho = config['CAMINHO_EXCEL']
    df = pd.read_excel(caminho, sheet_name='Volume Actual', header=1)
    df = _corrigir_colunas_mojibake(df)

    # Primeira coluna → Veículo
    primeira_col = df.columns[0]
    if str(primeira_col).strip().lower() not in ('veículo', 'veiculo', 'modelo'):
        df = df.rename(columns={primeira_col: 'Veículo'})

    # Limpar
    df['Veículo'] = df['Veículo'].astype(str).str.strip()
    df = df[~df['Veículo'].str.lower().isin(['total', 'nan', '', 'none'])]

    # Remover Ano se existir
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # Detectar colunas de meses
    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        # Tentar pegar tudo que não é Veículo
        colunas_meses = [c for c in df.columns if c != 'Veículo']

    # Melt
    df = df.melt(id_vars=['Veículo'], value_vars=colunas_meses,
                 var_name='Período', value_name='Volume')
    df['Período'] = df['Período'].apply(_normalizar_periodo)
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)

    print(f"   ✅ {len(df):,} linhas de volume veículos")
    print(f"   Veículos: {sorted(df['Veículo'].unique())}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 4 — TEMPO VEÍCULOS (Actual)
# ═══════════════════════════════════════════════════════════════

def fase4_tempo_veiculos(config: Dict, df_vol: pd.DataFrame) -> pd.DataFrame:
    """
    Lê aba 'EST veículos - Actual' e cruza com volumes → Tempo Veic.

    Retorna DataFrame com colunas:
      Oficina, Veículo, EST, Período, Volume, Tempo Veic
    """
    print("\n📊 FASE 4 — Tempo veículos (EST × Volume)")

    caminho = config['CAMINHO_EXCEL']
    df = pd.read_excel(caminho, sheet_name='EST veículos - Actual', header=1)
    df = _corrigir_colunas_mojibake(df)

    # Renomear colunas
    for c in df.columns:
        cn = _normalizar_nome_coluna(c)
        if cn == 'oficina':
            df = df.rename(columns={c: 'Oficina'})
        elif cn == 'est':
            df = df.rename(columns={c: 'EST'})
        elif 've' in cn and 'cul' in cn:
            df = df.rename(columns={c: 'Veículo'})

    _exigir_colunas(df, ['Oficina', 'EST'], "aba 'EST veículos - Actual'")

    # Garantir coluna Veículo
    if 'Veículo' not in df.columns:
        # Tentar encontrar
        for c in df.columns:
            cs = str(c).lower()
            if 'modelo' in cs or ('ve' in cs and ('cu' in cs or 'íc' in cs)):
                df = df.rename(columns={c: 'Veículo'})
                break

    if 'Veículo' not in df.columns:
        raise ValueError("❌ Coluna 'Veículo' não encontrada na aba 'EST veículos - Actual'")

    # Manter apenas colunas necessárias
    df = df[['Oficina', 'Veículo', 'EST']].copy()
    df['EST'] = pd.to_numeric(df['EST'], errors='coerce').fillna(0)
    df['Veículo'] = df['Veículo'].astype(str).str.strip()
    df = df[df['Oficina'].notna() & (df['Oficina'] != '')]

    # Merge com volume
    merge_cols = ['Veículo']
    if 'Oficina' in df_vol.columns:
        merge_cols = ['Oficina', 'Veículo']

    df_merge = pd.merge(df, df_vol, on=merge_cols, how='inner')
    df_merge['Tempo Veic'] = df_merge['Volume'] * df_merge['EST']

    print(f"   ✅ {len(df_merge):,} linhas de tempo veículos")
    print(f"   Oficinas: {sorted(df_merge['Oficina'].unique())}")

    return df_merge


# ═══════════════════════════════════════════════════════════════
#  FASE 5 — RATEIO FA
# ═══════════════════════════════════════════════════════════════

def fase5_rateio_fa(config: Dict, df_fa: pd.DataFrame, df_tempo_veic: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Rateio FA por oficina/período.
    Mesma lógica do Budget (fase7):
      - Automático (BS, PS, PL): Rateio FA = Tempo FA / (Tempo FA + Tempo Veic)
      - Manual (QY, GS, SM): Rateio FA = fator_manual × taxa_pdr_global

    Retorna DataFrame com colunas:
      Oficina, Período, Rateio FA
    """
    print("\n📊 FASE 5 — Rateio FA")

    # Agregar Tempo FA por (Oficina, Período)
    tfa = df_fa.groupby(['Oficina', 'Período'], as_index=False)['Tempo FA'].sum()
    tfa = tfa.rename(columns={'Tempo FA': 'Tempo FA Total'})

    # Agregar Tempo Veic por (Oficina, Período)
    tvc = df_tempo_veic.groupby(['Oficina', 'Período'], as_index=False)['Tempo Veic'].sum()
    tvc = tvc.rename(columns={'Tempo Veic': 'Tempo Veic Total'})

    # Merge outer
    df = pd.merge(tfa, tvc, on=['Oficina', 'Período'], how='outer')
    df['Tempo FA Total'] = df['Tempo FA Total'].fillna(0)
    df['Tempo Veic Total'] = df['Tempo Veic Total'].fillna(0)

    # ── Rateio automático (BS, PS, PL) ──
    mask_auto = df['Oficina'].isin(OFICINAS_RATEIO_AUTOMATICO)
    denominador = df['Tempo FA Total'] + df['Tempo Veic Total']
    df.loc[mask_auto, 'Rateio FA'] = np.where(
        denominador[mask_auto] != 0,
        df.loc[mask_auto, 'Tempo FA Total'] / denominador[mask_auto],
        0.0
    )

    # ── Rateio manual (QY, GS, SM) ──
    rateios_manuais = config['RATEIOS_MANUAIS']

    # Taxa PdR global por período
    periodos = df['Período'].unique()
    for periodo in periodos:
        dp = df[df['Período'] == periodo]

        # TFA global (todas as oficinas que têm FA neste período)
        tfa_global = dp['Tempo FA Total'].sum()

        # TVC global (excluindo oficinas GS, SM do denominador)
        mask_denom = ~dp['Oficina'].isin(OFICINAS_EXCLUIR_DENOM_TAXA_PDR)
        tvc_global = dp.loc[mask_denom, 'Tempo Veic Total'].sum()

        # Taxa PdR = TFA_global / TVC_global
        if tvc_global != 0:
            taxa_pdr = tfa_global / tvc_global
        else:
            taxa_pdr = 0.0

        # Aplicar a cada oficina manual
        for ofi in OFICINAS_RATEIO_MANUAL:
            fator = rateios_manuais.get(ofi, 0.0)
            rateio = fator * taxa_pdr

            mask = (df['Oficina'] == ofi) & (df['Período'] == periodo)
            if mask.any():
                df.loc[mask, 'Rateio FA'] = rateio
            else:
                # Criar linha
                nova = pd.DataFrame([{
                    'Oficina': ofi, 'Período': periodo,
                    'Tempo FA Total': 0, 'Tempo Veic Total': 0,
                    'Rateio FA': rateio
                }])
                df = pd.concat([df, nova], ignore_index=True)

    df['Rateio FA'] = df['Rateio FA'].fillna(0)

    # Limitar ao intervalo [0, 1]
    df['Rateio FA'] = df['Rateio FA'].clip(0, 1)

    print(f"   ✅ {len(df):,} linhas de rateio")
    print(f"   Oficinas automáticas: {OFICINAS_RATEIO_AUTOMATICO}")
    print(f"   Oficinas manuais: {OFICINAS_RATEIO_MANUAL}")

    return df[['Oficina', 'Período', 'Rateio FA']]


# ═══════════════════════════════════════════════════════════════
#  FASE 6 — CUSTO FA
# ═══════════════════════════════════════════════════════════════

def fase6_custo_fa(df_principal: pd.DataFrame, df_rateio: pd.DataFrame) -> pd.DataFrame:
    """
    Merge Rateio FA na tabela principal e calcula Custo FA.
    Linhas Redis (_fonte_redis=True) NÃO participam do rateio FA:
      - Custo FA = 0 para Redis (Redis vai integralmente para FP)
      - Apenas linhas Sapiens recebem o rateio FA normalmente

    Retorna df_principal com colunas adicionais:
      Rateio FA, Custo FA
    """
    print("\n📊 FASE 6 — Custo FA")

    df = pd.merge(df_principal, df_rateio, on=['Oficina', 'Período'], how='left')
    df['Rateio FA'] = df['Rateio FA'].fillna(0)

    # ═══ Redis NÃO participa do rateio FA ═══
    # No Excel, massa FA é calculada apenas sobre massa primária (Sapiens).
    # Redis é subtraído integralmente do FP, sem passar pelo FA.
    if '_fonte_redis' in df.columns:
        mask_redis = df['_fonte_redis'] == True
        n_redis = mask_redis.sum()
        if n_redis > 0:
            df.loc[mask_redis, 'Rateio FA'] = 0
            print(f"   ℹ️ {n_redis:,} linhas Redis com Rateio FA = 0 (vão integralmente para FP)")

    # Custo FA
    df['Custo FA'] = df['Rateio FA'] * df['Despesa Primaria']

    print(f"   ✅ Custo FA total: R$ {df['Custo FA'].sum():,.2f}")
    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 7 — CUSTO FP
# ═══════════════════════════════════════════════════════════════

def fase7_custo_fp(df_principal: pd.DataFrame) -> pd.DataFrame:
    """
    Custo FP = Despesa Primaria − Custo FA.
    Prova cruzada: DP − FA − FP ≈ 0 para cada linha.
    """
    print("\n📊 FASE 7 — Custo FP")

    df = df_principal.copy()
    df['Custo FP'] = df['Despesa Primaria'] - df['Custo FA']

    # Prova cruzada
    diff = (df['Despesa Primaria'] - df['Custo FA'] - df['Custo FP']).abs()
    erros = (diff > 0.01).sum()
    if erros > 0:
        print(f"   ⚠️ {erros:,} linhas com diferença > 0,01 na prova cruzada")
    else:
        print(f"   ✅ Prova cruzada: todas as {len(df):,} linhas OK")

    # Resumo
    dp_total = df['Despesa Primaria'].sum()
    fa_total = df['Custo FA'].sum()
    fp_total = df['Custo FP'].sum()

    print(f"   Despesa Primária: R$ {dp_total:,.2f}")
    print(f"   Custo FA:         R$ {fa_total:,.2f}")
    print(f"   Custo FP:         R$ {fp_total:,.2f}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 8 — D&A DEDICADO (do Budget)
# ═══════════════════════════════════════════════════════════════

def fase8_dea_dedicado(config: Dict) -> Optional[pd.DataFrame]:
    """
    Carrega D&A Dedicado do parquet Budget.
    Para Real, a D&A dedicada vem do mesmo orçamento (não muda mês a mês).

    Retorna DataFrame ou None se Budget não processado.
    """
    print("\n📊 FASE 8 — D&A Dedicado (do Budget)")

    if not config['TEM_DEA_BUD']:
        print("   ⚠️ Parquet de D&A Dedicado Budget não encontrado")
        return None

    df = pd.read_parquet(config['CAMINHO_DEA_BUD'])

    # Remover coluna Ano se existir (será readicionada no salvamento)
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    print(f"   ✅ {len(df):,} linhas de D&A Dedicado carregadas do Budget")
    if 'D&A dedicado' in df.columns:
        print(f"   D&A total: R$ {df['D&A dedicado'].sum():,.2f}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 9 — FP SEM DEDICADA
# ═══════════════════════════════════════════════════════════════

def fase9_fp_sem_dedicada(df_principal: pd.DataFrame, df_dea: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Calcula FP sem Dedicada = Custo FP − D&A dedicado.

    Se D&A não disponível, D&A = 0 e FP sem Dedicada = Custo FP.
    """
    print("\n📊 FASE 9 — FP sem Dedicada")

    df = df_principal.copy()

    if df_dea is not None and 'D&A dedicado' in df_dea.columns:
        # Agregar D&A por (Oficina, Account, Período) — somar todos os veículos
        cols_merge = ['Oficina', 'Account', 'Período']
        cols_merge = [c for c in cols_merge if c in df_dea.columns]

        dea_agg = df_dea.groupby(cols_merge, as_index=False)['D&A dedicado'].sum()
        dea_agg = dea_agg.rename(columns={'D&A dedicado': '_dea_grupo'})

        df = pd.merge(df, dea_agg, on=cols_merge, how='left')
        df['_dea_grupo'] = df['_dea_grupo'].fillna(0)

        # ── Distribuir D&A pro-rata pelo Custo FP de cada linha ──
        # Cada grupo (Oficina, Account, Período) recebe 1 total de D&A;
        # precisamos repartir entre as N linhas do grupo.
        _total_fp_grupo = df.groupby(cols_merge)['Custo FP'].transform('sum')
        df['D&A dedicado'] = np.where(
            _total_fp_grupo != 0,
            df['_dea_grupo'] * (df['Custo FP'] / _total_fp_grupo),
            0.0,
        )
        df.drop(columns=['_dea_grupo'], inplace=True)
    else:
        df['D&A dedicado'] = 0
        print("   ℹ️ Sem D&A Dedicado — usando zero")

    df['FP sem Dedicada'] = df['Custo FP'] - df['D&A dedicado']

    print(f"   ✅ FP sem Dedicada total: R$ {df['FP sem Dedicada'].sum():,.2f}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 10 — SALVAMENTO PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def fase10_salvamento(config: Dict, df_principal: pd.DataFrame,
                      df_fa: pd.DataFrame, df_tempo_veic: pd.DataFrame,
                      df_vol: pd.DataFrame, df_dea: Optional[pd.DataFrame]) -> Dict[str, str]:
    """
    Salva parquets principais em dados/TC_Principal/{ano}/.

    Arquivos (sem sufixo _BUD):
      df_principal.parquet
      df_volume_fa.parquet
      df_tempo_veiculos.parquet
      df_vol_veiculos.parquet
      df_dea_dedicado.parquet (se disponível)
    """
    print("\n💾 FASE 10 — Salvamento principal")

    pasta = config['PASTA_SAIDA']
    ano = config['ANO_ATUAL']
    arquivos = {}

    dados_para_salvar = {
        'df_principal.parquet': df_principal,
        'df_volume_fa.parquet': df_fa,
        'df_tempo_veiculos.parquet': df_tempo_veic,
        'df_vol_veiculos.parquet': df_vol,
    }

    if df_dea is not None:
        dados_para_salvar['df_dea_dedicado.parquet'] = df_dea

    for nome, df in dados_para_salvar.items():
        df_out = df.copy()
        df_out['Ano'] = ano
        df_out = normalizar_tipos_para_parquet(df_out)

        caminho = os.path.join(pasta, nome)
        df_out.to_parquet(caminho, index=False, engine='pyarrow')
        arquivos[nome] = caminho
        print(f"   ✅ {nome} → {caminho} ({len(df_out):,} linhas)")

    return arquivos


# ═══════════════════════════════════════════════════════════════
#  FASE 10B — PARQUET SAPIENS DETALHADO (todas as colunas)
# ═══════════════════════════════════════════════════════════════

def fase10b_sapiens_detalhado(config: Dict, df_principal: pd.DataFrame) -> str:
    """
    Salva df_tc_sapiens.parquet com TODAS as colunas do Sapiens original
    mais as colunas calculadas (Custo FA, Custo FP, etc.).

    Objetivo: permitir drill-down completo na aba "Dados Detalhados"
    com colunas como Texto breve, Fornecedor, Material, Doc.compra etc.
    """
    print("\n💾 FASE 10B — Parquet Sapiens Detalhado")

    pasta = config['PASTA_SAIDA']
    ano = config['ANO_ATUAL']

    df = df_principal.copy()
    df['Ano'] = ano

    # Remover colunas internas/temporárias
    cols_drop = [c for c in df.columns if c.startswith('_')]
    if cols_drop:
        df = df.drop(columns=cols_drop)
        print(f"   ℹ️ Removidas {len(cols_drop)} colunas internas: {cols_drop}")

    # Organizar colunas: identificação → detalhe Sapiens → valores calculados
    cols_id = ['Ano', 'Período', 'Oficina', 'Veículo']
    cols_classificacao = ['Account', 'Type 05', 'Type 06', 'Nºconta']
    cols_detalhe = ['Centrocst', 'Nºdoc.ref.', 'Dt.lçto.', 'Doc.compra',
                    'Texto breve', 'Fornecedor', 'Material', 'Usuário',
                    'Fornec.', 'Tipo', 'USI', 'QTD']
    cols_valores = ['Despesa Primaria', 'Rateio FA', 'Custo FA', 'Custo FP',
                    'D&A dedicado', 'FP sem Dedicada']

    # Montar ordem final (apenas colunas que realmente existem)
    ordem_desejada = cols_id + cols_classificacao + cols_detalhe + cols_valores
    cols_ordenadas = [c for c in ordem_desejada if c in df.columns]
    # Adicionar quaisquer colunas extras não listadas acima
    cols_extras = [c for c in df.columns if c not in cols_ordenadas]
    cols_final = cols_ordenadas + cols_extras

    df = df[cols_final]
    df = normalizar_tipos_para_parquet(df)

    caminho = os.path.join(pasta, 'df_tc_sapiens.parquet')
    df.to_parquet(caminho, index=False, engine='pyarrow')

    print(f"   ✅ df_tc_sapiens.parquet → {caminho}")
    print(f"   📊 {len(df):,} linhas × {len(df.columns)} colunas")
    print(f"   Colunas: {list(df.columns)}")

    return caminho


# ═══════════════════════════════════════════════════════════════
#  FASE 11 — CUSTO FP SEM D&A (isolamento)
# ═══════════════════════════════════════════════════════════════

def fase11_custo_fp_sem_da(df_principal: pd.DataFrame) -> pd.DataFrame:
    """
    Isola tabela intermediária para rastreabilidade.
    Mesma lógica do Budget fase13.
    """
    print("\n📊 FASE 11 — Custo FP sem D&A (isolamento)")

    cols_dim = ['Oficina', 'Account', 'Período', 'Type 05', 'Type 06', 'Custo']
    cols_dim = [c for c in cols_dim if c in df_principal.columns]
    cols_val = ['Custo FP', 'D&A dedicado', 'FP sem Dedicada']
    cols_val = [c for c in cols_val if c in df_principal.columns]

    df = df_principal[cols_dim + cols_val].copy()
    print(f"   ✅ {len(df):,} linhas isoladas")
    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 12 — PERCENTUAL RATEIO VEÍCULOS
# ═══════════════════════════════════════════════════════════════

def fase12_percentual_rateio_veiculos(df_tempo_veic: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula percentual de rateio por veículo baseado no tempo.
    Mesma lógica do Budget fase14.
    """
    print("\n📊 FASE 12 — Percentual de rateio veículos")

    _exigir_colunas(df_tempo_veic, ['Oficina', 'Veículo', 'Período', 'Tempo Veic'],
                    "Tempo Veículos")

    df = df_tempo_veic.copy()

    # Total tempo por (Oficina, Período)
    total = df.groupby(['Oficina', 'Período'], as_index=False)['Tempo Veic'].sum()
    total = total.rename(columns={'Tempo Veic': 'Total_Tempo_Oficina'})

    df = pd.merge(df, total, on=['Oficina', 'Período'], how='left')
    df['Percentual'] = np.where(
        df['Total_Tempo_Oficina'] != 0,
        df['Tempo Veic'] / df['Total_Tempo_Oficina'],
        0.0
    )

    # Validação: soma percentuais por (Oficina, Período) ≈ 1.0
    soma_pct = df.groupby(['Oficina', 'Período'])['Percentual'].sum()
    erros_pct = (soma_pct - 1.0).abs() > 0.01
    if erros_pct.any():
        n_erros = erros_pct.sum()
        print(f"   ⚠️ {n_erros} grupos com Σ percentuais ≠ 1.0")
    else:
        print(f"   ✅ Σ percentuais = 1.0 para todos os grupos")

    print(f"   {len(df):,} linhas de percentual rateio")
    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 13 — CUSTO RATEADO VEÍCULOS
# ═══════════════════════════════════════════════════════════════

def fase13_custo_rateado_veiculos(df_principal: pd.DataFrame,
                                   df_percentual: pd.DataFrame) -> pd.DataFrame:
    """
    Rateia FP sem Dedicada por veículo usando percentuais de tempo.
    Mesma lógica do Budget fase15.
    """
    print("\n📊 FASE 13 — Custo rateado por veículo")

    pct_cols = ['Oficina', 'Veículo', 'Período', 'Percentual']
    df_pct = df_percentual[pct_cols].copy()

    # Merge: expande principal para granularidade de veículo
    df = pd.merge(df_principal, df_pct, on=['Oficina', 'Período'], how='left')

    # Linhas sem veículo → distribuir pro-rata entre veículos do período
    mask_sem = df['Veículo'].isna()
    if mask_sem.any():
        n_sem = mask_sem.sum()
        print(f"   ⚠️ {n_sem} linhas sem veículo — distribuindo pro-rata")
        df_com = df[~mask_sem].copy()
        df_sem = df[mask_sem].drop(columns=['Veículo', 'Percentual']).copy()
        # Calcular distribuição média por Período entre veículos conhecidos
        dist_periodo = (
            df_pct.groupby(['Período', 'Veículo'])['Percentual']
            .mean().reset_index()
        )
        # Normalizar para que a soma por Período = 1.0
        soma_per = dist_periodo.groupby('Período')['Percentual'].transform('sum')
        dist_periodo['Percentual'] = dist_periodo['Percentual'] / soma_per.replace(0, 1)
        # Expandir linhas sem veículo usando distribuição do período
        df_sem_expanded = pd.merge(df_sem, dist_periodo, on='Período', how='left')
        # Se ainda restarem sem veículo (período sem nenhum veículo conhecido), usar dist global
        mask_still = df_sem_expanded['Veículo'].isna()
        if mask_still.any():
            veiculos_unicos = df_pct['Veículo'].unique()
            n_veic = max(1, len(veiculos_unicos))
            linhas_orfas = df_sem_expanded[mask_still].drop(columns=['Veículo', 'Percentual'])
            expansoes = []
            for v in veiculos_unicos:
                tmp = linhas_orfas.copy()
                tmp['Veículo'] = v
                tmp['Percentual'] = 1.0 / n_veic
                expansoes.append(tmp)
            df_sem_expanded = pd.concat(
                [df_sem_expanded[~mask_still]] + expansoes, ignore_index=True
            )
        df = pd.concat([df_com, df_sem_expanded], ignore_index=True)
    
    df['Percentual'] = df['Percentual'].fillna(0)

    # Custo Rateado
    df['Custo Rateado'] = df['FP sem Dedicada'] * df['Percentual']

    # Validação
    fp_total = df_principal['FP sem Dedicada'].sum()
    rateado_total = df['Custo Rateado'].sum()
    diff = abs(fp_total - rateado_total)
    if diff > 1.0:
        print(f"   ⚠️ Diferença no fechamento: R$ {diff:,.2f}")
    else:
        print(f"   ✅ Fechamento OK (diff: R$ {diff:,.4f})")

    print(f"   {len(df):,} linhas de custo rateado")
    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 14 — CUSTO FP VEÍCULOS
# ═══════════════════════════════════════════════════════════════

def fase14_custo_fp_veiculo(df_custo_rateado: pd.DataFrame,
                             df_dea: Optional[pd.DataFrame],
                             df_principal: pd.DataFrame) -> pd.DataFrame:
    """
    Custo FP Veiculo = Custo Rateado + D&A dedicado.
    Mesma lógica do Budget fase16.
    """
    print("\n📊 FASE 14 — Custo FP por veículo")

    df = df_custo_rateado.copy()

    if df_dea is not None and 'Veículo' in df_dea.columns and 'D&A dedicado' in df_dea.columns:
        # Agregar D&A por (Oficina, Veículo, Account, Período)
        cols_merge_dea = ['Oficina', 'Veículo', 'Account', 'Período']
        cols_merge_dea = [c for c in cols_merge_dea if c in df_dea.columns]
        dea_agg = df_dea.groupby(cols_merge_dea, as_index=False)['D&A dedicado'].sum()
        dea_agg = dea_agg.rename(columns={'D&A dedicado': '_dea_grupo'})

        # Merge com custo rateado
        cols_merge = [c for c in cols_merge_dea if c in df.columns]
        df = pd.merge(df, dea_agg, on=cols_merge, how='left', suffixes=('', '_dea'))
        df['_dea_grupo'] = df['_dea_grupo'].fillna(0)

        # ── Distribuir D&A pro-rata entre as linhas do grupo ──
        _n_rows = df.groupby(cols_merge)['Custo Rateado'].transform('count')
        df['D&A dedicado'] = df['_dea_grupo'] / _n_rows.replace(0, 1)
        df.drop(columns=['_dea_grupo'], inplace=True)
    else:
        if 'D&A dedicado' not in df.columns:
            df['D&A dedicado'] = 0

    df['Custo FP Veiculo'] = df['Custo Rateado'] + df['D&A dedicado']

    # Validação
    fp_original = df_principal['Custo FP'].sum()
    fp_veiculo = df['Custo FP Veiculo'].sum()
    diff = abs(fp_original - fp_veiculo)
    if diff > 1.0:
        print(f"   ⚠️ Diferença entre Custo FP original e veículos: R$ {diff:,.2f}")
    else:
        print(f"   ✅ Fechamento OK (diff: R$ {diff:,.4f})")

    print(f"   {len(df):,} linhas de custo FP veículos")
    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 15 — CPU VEÍCULOS
# ═══════════════════════════════════════════════════════════════

def fase15_cpu_veiculo(df_custo_fp_veiculo: pd.DataFrame,
                       df_vol: pd.DataFrame) -> tuple:
    """
    CPU = Custo FP Veiculo / Volume (protegido contra divisão por zero).
    Mesma lógica do Budget fase17.

    Retorna: (df_cpu, df_cpu_detalhe)
    """
    print("\n📊 FASE 15 — CPU por veículo")

    # Agregar custo por (Veículo, Período)
    custo_agg = df_custo_fp_veiculo.groupby(
        ['Veículo', 'Período'], as_index=False
    )['Custo FP Veiculo'].sum()

    # Agregar volume por (Veículo, Período)
    vol_agg = df_vol.groupby(
        ['Veículo', 'Período'], as_index=False
    )['Volume'].sum()

    # Merge
    df_cpu = pd.merge(custo_agg, vol_agg, on=['Veículo', 'Período'], how='left')
    df_cpu['Volume'] = df_cpu['Volume'].fillna(0)

    # CPU
    df_cpu['CPU'] = np.where(
        df_cpu['Volume'] != 0,
        df_cpu['Custo FP Veiculo'] / df_cpu['Volume'],
        0.0
    )

    # Detalhe (com Oficina/Account para debug)
    detalhe_cols = ['Oficina', 'Veículo', 'Account', 'Período', 'Custo FP Veiculo']
    detalhe_cols = [c for c in detalhe_cols if c in df_custo_fp_veiculo.columns]
    df_detalhe = df_custo_fp_veiculo[detalhe_cols].copy()

    print(f"   ✅ {len(df_cpu):,} linhas de CPU")
    print(f"   Veículos: {sorted(df_cpu['Veículo'].unique())}")

    return df_cpu, df_detalhe


# ═══════════════════════════════════════════════════════════════
#  FASE 16 — SALVAMENTO VEÍCULOS
# ═══════════════════════════════════════════════════════════════

def fase16_salvamento_veiculos(config: Dict, df_fp_sem_da: pd.DataFrame,
                                df_percentual: pd.DataFrame,
                                df_custo_rateado: pd.DataFrame,
                                df_custo_fp_veiculo: pd.DataFrame,
                                df_cpu: pd.DataFrame) -> Dict[str, str]:
    """
    Salva parquets de veículos em dados/TC_Principal/{ano}/.
    Nomes sem sufixo _BUD.
    """
    print("\n💾 FASE 16 — Salvamento veículos")

    pasta = config['PASTA_SAIDA']
    ano = config['ANO_ATUAL']
    arquivos = {}

    dados_para_salvar = {
        'df_veiculos_fp_sem_da.parquet': df_fp_sem_da,
        'df_veiculos_percentual_rateio.parquet': df_percentual,
        'df_veiculos_custo_rateado.parquet': df_custo_rateado,
        'df_veiculos_custo_fp.parquet': df_custo_fp_veiculo,
        'df_veiculos_cpu.parquet': df_cpu,
    }

    for nome, df in dados_para_salvar.items():
        df_out = df.copy()
        df_out['Ano'] = ano
        df_out = normalizar_tipos_para_parquet(df_out)

        caminho = os.path.join(pasta, nome)
        df_out.to_parquet(caminho, index=False, engine='pyarrow')
        arquivos[nome] = caminho
        print(f"   ✅ {nome} → {caminho} ({len(df_out):,} linhas)")

    return arquivos


# ═══════════════════════════════════════════════════════════════
#  FASE 17 — COMPARATIVO REAL × BUDGET
# ═══════════════════════════════════════════════════════════════

def fase17_comparativo(config: Dict, df_principal_real: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Cria tabela comparativa Real × Budget por Oficina/Período.
    Carrega df_principal_BUD.parquet e compara.

    Colunas: Oficina, Período, DP_Real, DP_Budget, Diff_DP,
             FA_Real, FA_Budget, Diff_FA, FP_Real, FP_Budget, Diff_FP
    """
    print("\n📊 FASE 17 — Comparativo Real × Budget")

    caminho_bud = os.path.join(config['PASTA_BUD'], 'df_principal_BUD.parquet')
    if not os.path.exists(caminho_bud):
        print("   ⚠️ Budget não processado — comparativo não disponível")
        return None

    df_bud = pd.read_parquet(caminho_bud)

    # Agregar por (Oficina, Período) para ambos
    cols_agg = ['Despesa Primaria', 'Custo FA', 'Custo FP']
    cols_agg = [c for c in cols_agg if c in df_principal_real.columns and c in df_bud.columns]

    real_agg = df_principal_real.groupby(['Oficina', 'Período'], as_index=False)[cols_agg].sum()
    bud_agg = df_bud.groupby(['Oficina', 'Período'], as_index=False)[cols_agg].sum()

    # Merge
    df = pd.merge(
        real_agg, bud_agg,
        on=['Oficina', 'Período'],
        how='outer',
        suffixes=('_Real', '_Budget')
    )

    # Calcular diferenças
    for col in cols_agg:
        col_real = f'{col}_Real'
        col_bud = f'{col}_Budget'
        col_diff = f'Diff_{col.split()[-1]}'
        df[col_real] = df[col_real].fillna(0)
        df[col_bud] = df[col_bud].fillna(0)
        df[col_diff] = df[col_real] - df[col_bud]

    # Salvar
    pasta = config['PASTA_SAIDA']
    ano = config['ANO_ATUAL']
    df['Ano'] = ano
    df_out = normalizar_tipos_para_parquet(df)

    caminho = os.path.join(pasta, 'df_comparativo_real_budget.parquet')
    df_out.to_parquet(caminho, index=False, engine='pyarrow')
    print(f"   ✅ Comparativo salvo → {caminho} ({len(df_out):,} linhas)")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 18 — VALIDAÇÃO FINAL
# ═══════════════════════════════════════════════════════════════

def validacao_final(config: Dict, arquivos: Dict[str, str]) -> None:
    """
    Validação de integridade dos parquets gerados.
    Verifica existência, coluna Ano, períodos e prova cruzada.
    """
    print("\n🔍 VALIDAÇÃO FINAL")
    print("=" * 60)

    erros = 0

    for nome, caminho in arquivos.items():
        if not os.path.exists(caminho):
            print(f"   ❌ Não encontrado: {caminho}")
            erros += 1
            continue

        df = pd.read_parquet(caminho)

        # Verificar coluna Ano
        if 'Ano' not in df.columns:
            print(f"   ⚠️ {nome}: coluna 'Ano' ausente")
        else:
            anos_unicos = df['Ano'].unique()
            if len(anos_unicos) != 1:
                print(f"   ⚠️ {nome}: múltiplos anos: {anos_unicos}")

        # Verificar Períodos
        if 'Período' in df.columns:
            n_periodos = df['Período'].nunique()
            if n_periodos < 12:
                print(f"   ℹ️ {nome}: {n_periodos} períodos (< 12)")

    # Prova cruzada no principal
    caminho_principal = arquivos.get('df_principal.parquet')
    if caminho_principal and os.path.exists(caminho_principal):
        df_p = pd.read_parquet(caminho_principal)
        if all(c in df_p.columns for c in ['Despesa Primaria', 'Custo FA', 'Custo FP']):
            diff = (df_p['Despesa Primaria'] - df_p['Custo FA'] - df_p['Custo FP']).abs()
            n_erros = (diff > 0.01).sum()
            if n_erros > 0:
                print(f"\n   ❌ Prova cruzada: {n_erros:,} linhas com DP − FA − FP ≠ 0")
                erros += 1
            else:
                print(f"\n   ✅ Prova cruzada OK: DP − FA − FP = 0 ({len(df_p):,} linhas)")

            # Métricas resumo
            dp = df_p['Despesa Primaria'].sum()
            fa = df_p['Custo FA'].sum()
            fp = df_p['Custo FP'].sum()

            print(f"\n   📊 RESUMO REAL:")
            print(f"      Despesa Primária:  R$ {dp:>18,.2f}")
            print(f"      Custo FA:          R$ {fa:>18,.2f}")
            print(f"      Custo FP:          R$ {fp:>18,.2f}")

            if 'FP sem Dedicada' in df_p.columns:
                fps = df_p['FP sem Dedicada'].sum()
                print(f"      FP sem Dedicada:   R$ {fps:>18,.2f}")

    if erros == 0:
        print(f"\n   ✅ Validação final concluída sem erros")
    else:
        print(f"\n   ⚠️ Validação final: {erros} problema(s) encontrado(s)")

    print("=" * 60)


# ═══════════════════════════════════════════════════════════════
#  CONFERÊNCIA AUTOMÁTICA (Excel × Parquet)
# ═══════════════════════════════════════════════════════════════

def executar_conferencias(ano: int, tipo: str = 'real') -> pd.DataFrame:
    """
    Confere dados processados (parquets) contra as mesmas abas-fonte do Excel
    usadas pela extração.

    Real  — fonte: Sapiens (header=1), massa-REDIS, Volume e EST PdR - Actual
            FA e FP são CALCULADOS (não lidos do Excel) → validados por prova cruzada.
    Budget — fonte: massa primária - BDG, massa-REDIS, Volume e EST PdR - BDG,
             massa FA - BDG, massa FP - BDG (melt idêntico à extração).

    Returns:
        DataFrame com colunas: Conferência, Excel, Parquet, Diferença, % Diff, Status
    """
    pasta_tc = os.path.join(_DATA_ROOT, str(ano))
    excel_path = os.path.join(pasta_tc, 'Reporting veículos.xlsx')
    resultados = []

    if not os.path.exists(excel_path):
        return pd.DataFrame([{'Conferência': 'ERRO', 'Status': '❌ Excel não encontrado'}])

    # ── helpers ────────────────────────────────────────────────
    def _add(nome, val_excel, val_parquet):
        diff = abs(val_excel - val_parquet)
        pct = (diff / abs(val_excel) * 100) if val_excel != 0 else 0
        status = "✅" if pct < 0.01 else ("⚠️" if pct < 1 else "❌")
        resultados.append({
            'Conferência': nome,
            'Excel': f"{val_excel:,.2f}",
            'Parquet': f"{val_parquet:,.2f}",
            'Diferença': f"{diff:,.2f}",
            '% Diff': f"{pct:.4f}%",
            'Status': status,
        })

    def _ler_aba_melt(sheet_name: str) -> pd.DataFrame:
        """Lê aba wide (oficina × meses) e faz melt — mesma lógica da extração."""
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        df = _corrigir_colunas_mojibake(df)
        colunas_meses = _detectar_colunas_meses(df)
        if not colunas_meses:
            return pd.DataFrame()
        if 'Ano' in df.columns:
            df = df.drop(columns=['Ano'])
        colunas_dim = [c for c in df.columns if c not in colunas_meses]
        df_m = df.melt(id_vars=colunas_dim, value_vars=colunas_meses,
                       var_name='Período', value_name='_valor')
        df_m['Período'] = df_m['Período'].apply(_normalizar_periodo)
        df_m['_valor'] = pd.to_numeric(df_m['_valor'], errors='coerce').fillna(0)
        if 'Oficina' in df_m.columns:
            df_m = df_m[df_m['Oficina'].notna() & (df_m['Oficina'] != '')]
            df_m = df_m[~df_m['Oficina'].isin(OFICINAS_INVALIDAS)]
        return df_m

    def _ler_sapiens() -> pd.DataFrame:
        """Lê aba Sapiens exatamente como fase1_sapiens (header=1, exclui Redis/oficinas inválidas)."""
        df = pd.read_excel(excel_path, sheet_name='Sapiens', header=1)
        df = _limpar_colunas_duplicadas(df)
        df = _corrigir_colunas_mojibake(df)
        df = _aplicar_alias_colunas(df, ALIAS_COLUNAS_SAPIENS)
        if 'Valor' in df.columns:
            df = df.rename(columns={'Valor': 'Despesa Primaria'})
        df['Despesa Primaria'] = pd.to_numeric(df['Despesa Primaria'], errors='coerce').fillna(0)
        df['Período'] = df['Período'].apply(_normalizar_periodo)
        df = df[df['Oficina'].notna() & (df['Oficina'] != '')]
        df = df[df['Despesa Primaria'] != 0]
        # Excluir Redis (vem de aba 'massa - REDIS')
        if 'Account' in df.columns:
            df = df[df['Account'] != 'Redis']
        # Excluir oficinas inválidas
        df = df[~df['Oficina'].isin(OFICINAS_INVALIDAS)]
        return df

    def _ler_redis_melt() -> pd.DataFrame:
        """Lê aba massa-REDIS exatamente como fase1b_redis (melt + oficinas válidas)."""
        df_m = _ler_aba_melt('massa - REDIS')
        # Remover linhas zeradas
        df_m = df_m[df_m['_valor'] != 0]
        return df_m

    # ── caminhos dos parquets ─────────────────────────────────
    if tipo == 'real':
        pasta_pq = pasta_tc
        pq_principal = os.path.join(pasta_pq, 'df_principal.parquet')
        pq_vol_fa = os.path.join(pasta_pq, 'df_volume_fa.parquet')
    else:
        pasta_pq = os.path.join(pasta_tc, 'BUD')
        pq_principal = os.path.join(pasta_pq, 'df_principal_BUD.parquet')
        pq_vol_fa = os.path.join(pasta_pq, 'df_volume_fa_BUD.parquet')

    try:
        # ══════════════════════════════════════
        # 1) Despesa Primária — fonte real da extração
        # ══════════════════════════════════════
        try:
            if tipo == 'real':
                # Real lê de 'Sapiens' (header=1, long format)
                df_sap = _ler_sapiens()
                val_excel = df_sap['Despesa Primaria'].sum()
            else:
                # BDG lê de 'massa primária - BDG' (melt)
                df_mp = _ler_aba_melt('massa primária - BDG')
                val_excel = df_mp['_valor'].sum()

            if os.path.exists(pq_principal):
                df_pq = pd.read_parquet(pq_principal)
                if '_fonte_redis' in df_pq.columns:
                    val_pq = df_pq[df_pq['_fonte_redis'] != True]['Despesa Primaria'].sum()
                else:
                    val_pq = df_pq['Despesa Primaria'].sum()
                _add(f"1) Despesa Primária ({tipo.upper()})", val_excel, val_pq)
            else:
                resultados.append({'Conferência': '1) Despesa Primária', 'Status': '⚠️ Parquet não encontrado'})
        except Exception as e:
            resultados.append({'Conferência': '1) Despesa Primária', 'Status': f'❌ {e}'})

        # ══════════════════════════════════════
        # 2) Redis (ambos leem de 'massa - REDIS')
        # ══════════════════════════════════════
        try:
            df_redis = _ler_redis_melt()
            val_excel_redis = df_redis['_valor'].sum() if len(df_redis) else 0

            if os.path.exists(pq_principal):
                df_pq = pd.read_parquet(pq_principal)
                if '_fonte_redis' in df_pq.columns:
                    val_pq_redis = -df_pq[df_pq['_fonte_redis'] == True]['Despesa Primaria'].sum()
                else:
                    val_pq_redis = 0
                _add("2) Redis", val_excel_redis, val_pq_redis)
        except Exception as e:
            resultados.append({'Conferência': '2) Redis', 'Status': f'❌ {e}'})

        # ══════════════════════════════════════
        # 3) Volume FA
        # ══════════════════════════════════════
        try:
            tab_vol = 'Volume e EST PdR - Actual' if tipo == 'real' else 'Volume e EST PdR - BDG'
            df_vol_raw = pd.read_excel(excel_path, sheet_name=tab_vol, header=None)
            hr = None
            for i in range(min(10, len(df_vol_raw))):
                vals = [str(v).lower() for v in df_vol_raw.iloc[i].values if pd.notna(v)]
                if any('oficina' in v for v in vals):
                    hr = i
                    break
            if hr is not None:
                df_vol = pd.read_excel(excel_path, sheet_name=tab_vol, header=hr)
                df_vol = _corrigir_colunas_mojibake(df_vol)
                meses_vol = _detectar_colunas_meses(df_vol)
                if 'Oficina' in df_vol.columns:
                    df_vol = df_vol[df_vol['Oficina'].notna() & (df_vol['Oficina'] != '')]
                    df_vol = df_vol[~df_vol['Oficina'].isin(OFICINAS_INVALIDAS)]
                val_excel_vol = sum(pd.to_numeric(df_vol[mc], errors='coerce').fillna(0).sum()
                                    for mc in meses_vol)
            else:
                val_excel_vol = 0

            if os.path.exists(pq_vol_fa):
                df_pq_vol = pd.read_parquet(pq_vol_fa)
                val_pq_vol = df_pq_vol['Vol FA'].sum()
                fonte = ""
                if '_fonte_volume_fa' in df_pq_vol.columns:
                    if 'BDG' in df_pq_vol['_fonte_volume_fa'].unique():
                        fonte = " [Fallback BDG]"
                _add(f"3) Volume FA{fonte}", val_excel_vol, val_pq_vol)
            else:
                resultados.append({'Conferência': '3) Volume FA', 'Status': '⚠️ Parquet não encontrado'})
        except Exception as e:
            resultados.append({'Conferência': '3) Volume FA', 'Status': f'❌ {e}'})

        # ══════════════════════════════════════
        # 4) Custo FA
        # ══════════════════════════════════════
        try:
            _suf = 'Actual' if tipo == 'real' else 'BDG'
            df_fa = _ler_aba_melt(f'massa FA - {_suf}')
            val_excel_fa = df_fa['_valor'].sum() if len(df_fa) else 0
            if os.path.exists(pq_principal):
                df_pq = pd.read_parquet(pq_principal)
                val_pq_fa = df_pq['Custo FA'].sum()
                _add(f"4) Custo FA ({tipo.upper()})", val_excel_fa, val_pq_fa)
        except Exception as e:
            resultados.append({'Conferência': '4) Custo FA', 'Status': f'❌ {e}'})

        # ══════════════════════════════════════
        # 5) Custo FP
        # ══════════════════════════════════════
        try:
            _suf = 'Actual' if tipo == 'real' else 'BDG'
            df_fp = _ler_aba_melt(f'massa FP - {_suf}')
            val_excel_fp = df_fp['_valor'].sum() if len(df_fp) else 0
            if os.path.exists(pq_principal):
                df_pq = pd.read_parquet(pq_principal)
                val_pq_fp = df_pq['Custo FP'].sum()
                _add(f"5) Custo FP ({tipo.upper()})", val_excel_fp, val_pq_fp)
        except Exception as e:
            resultados.append({'Conferência': '5) Custo FP', 'Status': f'❌ {e}'})

        # ══════════════════════════════════════
        # 6) Prova cruzada DP = FA + FP  (linha a linha)
        # ══════════════════════════════════════
        if os.path.exists(pq_principal):
            df_pq = pd.read_parquet(pq_principal)
            if all(c in df_pq.columns for c in ['Despesa Primaria', 'Custo FA', 'Custo FP']):
                diff_arr = (df_pq['Despesa Primaria'] - df_pq['Custo FA'] - df_pq['Custo FP']).abs()
                n_erros = (diff_arr > 0.01).sum()
                status = "✅" if n_erros == 0 else "❌"
                resultados.append({
                    'Conferência': '6) Prova cruzada (DP = FA + FP)',
                    'Excel': f"{len(df_pq):,} linhas",
                    'Parquet': f"{n_erros:,} erros",
                    'Diferença': f"{diff_arr.sum():,.2f}",
                    '% Diff': '-',
                    'Status': status,
                })

    except Exception as e:
        resultados.append({'Conferência': 'ERRO GERAL', 'Status': f'❌ {e}'})

    df_result = pd.DataFrame(resultados)
    for col in ['Conferência', 'Excel', 'Parquet', 'Diferença', '% Diff', 'Status']:
        if col not in df_result.columns:
            df_result[col] = '-'
    df_result = df_result.fillna('-')

    return df_result


# ═══════════════════════════════════════════════════════════════
#  CONSOLIDAÇÃO HISTÓRICO MULTI-ANO
# ═══════════════════════════════════════════════════════════════

def consolidar_historico_tc_veiculos(tipo: str = 'real') -> list:
    """Consolida parquets de todos os anos em histórico multi-ano.

    Args:
        tipo: 'real' ou 'budget'

    Returns:
        Lista de mensagens de resultado.
    """
    resultados = []
    pasta_base = _DATA_ROOT

    # Descobrir anos disponíveis
    anos = []
    if os.path.exists(pasta_base):
        for item in sorted(os.listdir(pasta_base)):
            caminho_item = os.path.join(pasta_base, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos.append(int(item))

    if not anos:
        return ["⚠️ Nenhum ano encontrado em dados/TC_Principal/"]

    pasta_hist = os.path.join(pasta_base, 'historico_consolidado')
    pasta_hist_bud = os.path.join(pasta_hist, 'BUD')
    os.makedirs(pasta_hist, exist_ok=True)
    os.makedirs(pasta_hist_bud, exist_ok=True)

    def _consolidar(mapa_arquivos: dict, pasta_destino: str):
        for nome_hist, (nome_fonte, subpasta) in mapa_arquivos.items():
            dfs = []
            for a in anos:
                if subpasta:
                    caminho = os.path.join(pasta_base, str(a), subpasta, nome_fonte)
                else:
                    caminho = os.path.join(pasta_base, str(a), nome_fonte)
                if os.path.exists(caminho):
                    try:
                        df = pd.read_parquet(caminho)
                        if 'Ano' not in df.columns:
                            df['Ano'] = a
                        dfs.append(df)
                    except Exception as e:
                        resultados.append(f"⚠️ Erro ao ler {caminho}: {e}")
            if dfs:
                df_final = pd.concat(dfs, ignore_index=True)
                destino = os.path.join(pasta_destino, nome_hist)
                df_final.to_parquet(destino, index=False)
                resultados.append(f"✅ {nome_hist}: {len(dfs)} ano(s) → {len(df_final):,} linhas")
            else:
                resultados.append(f"⚠️ {nome_hist}: nenhum dado encontrado")

    if tipo == 'real':
        _consolidar({
            'df_principal_historico.parquet': ('df_principal.parquet', ''),
            'df_vol_historico.parquet': ('df_vol_veiculos.parquet', ''),
            'df_cpu_historico.parquet': ('df_veiculos_cpu.parquet', ''),
            'df_veiculos_custo_fp_historico.parquet': ('df_veiculos_custo_fp.parquet', ''),
        }, pasta_hist)
    elif tipo == 'budget':
        _consolidar({
            'df_principal_historico_BUD.parquet': ('df_principal_BUD.parquet', 'BUD'),
            'df_vol_historico_BUD.parquet': ('df_vol_veiculos_BUD.parquet', 'BUD'),
            'df_cpu_historico_BUD.parquet': ('df_veiculos_cpu_BUD.parquet', 'BUD'),
            'df_veiculos_custo_fp_historico_BUD.parquet': ('df_veiculos_custo_fp_BUD.parquet', 'BUD'),
        }, pasta_hist_bud)

    return resultados


# ═══════════════════════════════════════════════════════════════
#  ORQUESTRADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def processar_veiculos_real(ano: Optional[int] = None,
                            progress_callback=None) -> Dict:
    """
    Pipeline completo de processamento Real (Sapiens) para TC Veículos.

    Args:
        ano: Ano de referência (default = ano atual)
        progress_callback: Função para reportar progresso (ex: st.write)

    Returns:
        Dict com todas as estruturas produzidas.
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)

    log("🚀 PROCESSAMENTO REAL (SAPIENS) — TC Veículos")
    log("=" * 60)

    # 0. Configuração
    log("⚙️ Configurando ambiente...")
    config = configurar_ambiente(ano)
    log(f"   Ano: {config['ANO_ATUAL']}")
    log(f"   Excel: {config['CAMINHO_EXCEL']}")
    log(f"   Saída: {config['PASTA_SAIDA']}")

    # 1. Sapiens → Despesa Primaria (sem Redis)
    log("\n📋 Fase 1/18: Leitura aba Sapiens (excluindo Redis)...")
    df_sapiens = fase1_sapiens(config)

    # 1B. massa - REDIS → linhas com Account real (receita)
    log("\n📋 Fase 1B/18: massa - REDIS (receita)...")
    df_redis = fase1b_redis(config)

    # Concatenar Sapiens + massa-REDIS
    if not df_redis.empty:
        # Garantir _fonte_redis=False nas linhas Sapiens
        if '_fonte_redis' not in df_sapiens.columns:
            df_sapiens = df_sapiens.copy()
            df_sapiens['_fonte_redis'] = False
        df_principal = pd.concat([df_sapiens, df_redis], ignore_index=True)
        df_principal['Despesa Primaria'] = df_principal['Despesa Primaria'].fillna(0)
        df_principal['_fonte_redis'] = df_principal['_fonte_redis'].fillna(False)
        log(f"   ✅ Sapiens ({len(df_sapiens):,}) + Redis ({len(df_redis):,}) = {len(df_principal):,} linhas")
    else:
        df_principal = df_sapiens
        df_principal['_fonte_redis'] = False
        log("   ℹ️ Sem dados Redis — usando apenas Sapiens")

    # 2. Volume e EST PdR (Actual)
    log("\n📋 Fase 2/18: Volume e EST PdR (Actual)...")
    df_fa = fase2_volume_est_fa(config)

    # 3. Volume veículos (Actual)
    log("\n📋 Fase 3/18: Volume veículos (Actual)...")
    df_vol = fase3_volume_veiculos(config)

    # 4. Tempo veículos (EST × Volume)
    log("\n📋 Fase 4/18: Tempo veículos (EST × Volume)...")
    df_tempo_veic = fase4_tempo_veiculos(config, df_vol)

    # 5. Rateio FA
    log("\n📋 Fase 5/18: Rateio FA...")
    df_rateio_fa = fase5_rateio_fa(config, df_fa, df_tempo_veic)

    # 6. Custo FA
    log("\n📋 Fase 6/18: Custo FA...")
    df_principal = fase6_custo_fa(df_principal, df_rateio_fa)

    # 7. Custo FP
    log("\n📋 Fase 7/18: Custo FP...")
    df_principal = fase7_custo_fp(df_principal)

    # 8. D&A Dedicado (do Budget)
    log("\n📋 Fase 8/18: D&A Dedicado (Budget)...")
    df_dea = fase8_dea_dedicado(config)

    # 9. FP sem Dedicada
    log("\n📋 Fase 9/18: FP sem Dedicada...")
    df_principal = fase9_fp_sem_dedicada(df_principal, df_dea)

    # 10. Salvamento principal
    log("\n📋 Fase 10/18: Salvamento principal...")
    arquivos = fase10_salvamento(config, df_principal, df_fa, df_tempo_veic, df_vol, df_dea)

    # 10B. Parquet Sapiens detalhado (todas as colunas)
    log("\n📋 Fase 10B: Parquet Sapiens detalhado (todas as colunas)...")
    caminho_sapiens = fase10b_sapiens_detalhado(config, df_principal)
    arquivos['df_tc_sapiens.parquet'] = caminho_sapiens

    # 11. Custo FP sem D&A (isolamento)
    log("\n📋 Fase 11/18: Custo FP sem D&A...")
    df_fp_sem_da = fase11_custo_fp_sem_da(df_principal)

    # 12. Percentual rateio veículos
    log("\n📋 Fase 12/18: Percentual rateio veículos...")
    df_percentual = fase12_percentual_rateio_veiculos(df_tempo_veic)

    # 13. Custo rateado por veículo
    log("\n📋 Fase 13/18: Custo rateado por veículo...")
    df_custo_rateado = fase13_custo_rateado_veiculos(df_principal, df_percentual)

    # 14. Custo FP veículos
    log("\n📋 Fase 14/18: Custo FP por veículo...")
    df_custo_fp_veiculo = fase14_custo_fp_veiculo(df_custo_rateado, df_dea, df_principal)

    # 15. CPU veículos
    log("\n📋 Fase 15/18: CPU por veículo...")
    df_cpu, df_cpu_detalhe = fase15_cpu_veiculo(df_custo_fp_veiculo, df_vol)

    # 16. Salvamento veículos
    log("\n📋 Fase 16/18: Salvamento veículos...")
    arquivos_veic = fase16_salvamento_veiculos(
        config, df_fp_sem_da, df_percentual,
        df_custo_rateado, df_custo_fp_veiculo, df_cpu
    )
    arquivos.update(arquivos_veic)

    # 17. Comparativo Real × Budget
    log("\n📋 Fase 17/18: Comparativo Real × Budget...")
    df_comparativo = fase17_comparativo(config, df_principal)

    # 18. Validação final
    log("\n📋 Fase 18/18: Validação final...")
    validacao_final(config, arquivos)

    # 19. Consolidação do histórico multi-ano (automática)
    log("\n📋 Consolidando histórico multi-ano...")
    try:
        msgs = consolidar_historico_tc_veiculos(tipo='real')
        for m in msgs:
            log(f"   {m}")
    except Exception as e:
        log(f"   ⚠️ Erro na consolidação: {e}")

    log("\n🎉 Processamento Real concluído!")

    return {
        'config': config,
        'arquivos': arquivos,
        'df_principal': df_principal,
        'df_fa': df_fa,
        'df_tempo_veiculos': df_tempo_veic,
        'df_vol': df_vol,
        'df_dea_dedicado': df_dea,
        'df_fp_sem_da': df_fp_sem_da,
        'df_percentual_rateio': df_percentual,
        'df_custo_rateado': df_custo_rateado,
        'df_custo_fp_veiculo': df_custo_fp_veiculo,
        'df_cpu': df_cpu,
        'df_comparativo': df_comparativo,
    }


# ═══════════════════════════════════════════════════════════════
#  EXECUÇÃO DIRETA
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    ano_arg = int(sys.argv[1]) if len(sys.argv) > 1 else None
    resultado = processar_veiculos_real(ano=ano_arg)
    print(f"\n📁 Arquivos gerados: {len(resultado['arquivos'])}")
    for nome, caminho in resultado['arquivos'].items():
        print(f"   {nome}: {caminho}")
