"""
Módulo de Processamento de Dados REAL (Sapiens) — TC Principal (Planta Principal)
Processa o arquivo 'Reporting veículos.xlsx' (aba Sapiens) para extrair custos
reais de produção de veículos.

Segue a mesma lógica metodológica do Budget (processamento_dados_veiculos_BUD.py),
com as seguintes diferenças:
  - Fonte de custo: aba 'Sapiens' (header=1) — dados já por linha/período
  - Redis: já está presente como linhas na aba Sapiens (Account='Redis')
  - Sem fase separada de Redis (não há aba massa - REDIS)
  - Volume/EST FA: aba 'Volume e EST PdR - Actual'
  - Tempo veículos: aba 'EST veículos - Actual'
  - Volume veículos: aba 'Volume Actual'
  - D&A Dedicado: reutiliza parquet gerado pelo Budget (df_dea_dedicado_BUD.parquet)
  - Saída: dados/TC_Principal/{ano}/ (raiz, sem subpasta BUD)

Fases:
   1. Sapiens             → Despesa Primaria (já por linha/período)
   2. Volume e EST PdR    → Vol FA + Tempo FA (aba Actual)
   3. Volume Actual       → volumes de veículos
   4. EST veículos Actual → merge com volume → Tempo Veic
   5. Rateio FA           → %FA por oficina (automático BS/PS/PL, manual QY/GS/SM)
   6. Custo FA            → Rateio FA × Despesa Primaria (Rateio FA=0 para Redis)
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

import pandas as pd
import numpy as np
import os
import json
import shutil
from datetime import datetime
from typing import Dict, Optional
import re
import unicodedata


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

    pasta_ano = os.path.join('dados', 'TC_Principal', str(ano))
    pasta_bud = os.path.join(pasta_ano, 'BUD')
    pasta_saida = pasta_ano  # Real salva na raiz do ano
    pasta_historico = os.path.join('dados', 'TC_Principal', 'historico_consolidado')

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
    Redis já está presente como linhas com Account='Redis'.

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

    # Remover coluna Ano se existir (será adicionada no salvamento)
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # Garantir colunas estruturais
    for col in ['Type 05', 'Type 06', 'Custo']:
        if col not in df.columns:
            print(f"   ⚠️ Coluna '{col}' não encontrada na aba Sapiens — criando com valor padrão")
            df[col] = ''

    # Estatísticas
    n_redis = (df['Account'] == 'Redis').sum()
    n_total = len(df)
    print(f"   ✅ {n_total:,} linhas lidas ({n_redis:,} Redis)")
    print(f"   Oficinas: {sorted(df['Oficina'].unique())}")
    print(f"   Períodos: {len(df['Período'].unique())}")
    print(f"   Despesa Primária total: R$ {df['Despesa Primaria'].sum():,.2f}")

    return df


# ═══════════════════════════════════════════════════════════════
#  FASE 2 — VOLUME E EST PdR (Actual)
# ═══════════════════════════════════════════════════════════════

def fase2_volume_est_fa(config: Dict) -> pd.DataFrame:
    """
    Lê aba 'Volume e EST PdR - Actual'.
    Mesma lógica do BDG (fase4), mas usando aba Actual.

    Retorna DataFrame com colunas:
      REF FER, Oficina, EST, Período, Vol FA, Tempo FA
    """
    print("\n📊 FASE 2 — Volume e EST PdR (Actual)")

    caminho = config['CAMINHO_EXCEL']

    # Detectar header (pode variar de posição)
    df_raw = pd.read_excel(caminho, sheet_name='Volume e EST PdR - Actual', header=None, nrows=10)
    header_row = 0
    for i in range(min(10, len(df_raw))):
        vals = [str(v).strip().lower() for v in df_raw.iloc[i].values if pd.notna(v)]
        if any('oficina' in v or 'ref' in v for v in vals):
            header_row = i
            break

    df = pd.read_excel(caminho, sheet_name='Volume e EST PdR - Actual', header=header_row)
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

    _exigir_colunas(df, ['Oficina', 'EST'], "aba 'Volume e EST PdR - Actual'")

    # Remover Ano se existir
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # EST numérico
    df['EST'] = pd.to_numeric(df['EST'], errors='coerce').fillna(0)

    # Detectar colunas de meses
    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        raise ValueError("❌ Sem colunas de meses na aba 'Volume e EST PdR - Actual'")

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

    print(f"   ✅ {len(df):,} linhas de volume FA")
    print(f"   Oficinas: {sorted(df['Oficina'].unique())}")

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

    Linhas Redis (Account='Redis') → Rateio FA = 0 (sem rateio).

    Retorna df_principal com colunas adicionais:
      Rateio FA, Custo FA
    """
    print("\n📊 FASE 6 — Custo FA")

    df = pd.merge(df_principal, df_rateio, on=['Oficina', 'Período'], how='left')
    df['Rateio FA'] = df['Rateio FA'].fillna(0)

    # Redis: forçar Rateio FA = 0
    mask_redis = df['Account'] == 'Redis'
    n_redis = mask_redis.sum()
    if n_redis > 0:
        df.loc[mask_redis, 'Rateio FA'] = 0
        print(f"   ℹ️  {n_redis:,} linhas Redis → Rateio FA = 0")

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

    Fórmula unificada (igual ao Budget redesenhado):
    Redis tem DP negativo e FA=0, logo FP = DP (negativo).

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
    redis_total = df[df['Account'] == 'Redis']['Despesa Primaria'].sum()

    print(f"   Despesa Primária: R$ {dp_total:,.2f}")
    print(f"   Custo FA:         R$ {fa_total:,.2f}")
    print(f"   Custo FP:         R$ {fp_total:,.2f}")
    print(f"   Redis (linhas):   R$ {redis_total:,.2f}")

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

        df = pd.merge(df, dea_agg, on=cols_merge, how='left')
        df['D&A dedicado'] = df['D&A dedicado'].fillna(0)
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

    # Linhas sem veículo → 'Sem Veículo', Percentual = 1.0
    mask_sem = df['Veículo'].isna()
    if mask_sem.any():
        df.loc[mask_sem, 'Veículo'] = 'Sem Veículo'
        df.loc[mask_sem, 'Percentual'] = 1.0

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

        # Merge com custo rateado
        cols_merge = [c for c in cols_merge_dea if c in df.columns]
        df = pd.merge(df, dea_agg, on=cols_merge, how='left', suffixes=('', '_dea'))

        # Resolver possíveis duplicatas
        if 'D&A dedicado_dea' in df.columns:
            df['D&A dedicado'] = df['D&A dedicado_dea'].fillna(0)
            df = df.drop(columns=['D&A dedicado_dea'])
        elif 'D&A dedicado' not in df.columns:
            df['D&A dedicado'] = 0
        else:
            df['D&A dedicado'] = df['D&A dedicado'].fillna(0)
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
            redis = df_p[df_p['Account'] == 'Redis']['Despesa Primaria'].sum() if 'Account' in df_p.columns else 0

            print(f"\n   📊 RESUMO REAL:")
            print(f"      Despesa Primária:  R$ {dp:>18,.2f}")
            print(f"      Custo FA:          R$ {fa:>18,.2f}")
            print(f"      Custo FP:          R$ {fp:>18,.2f}")
            print(f"      Redis (linhas):    R$ {redis:>18,.2f}")

            if 'FP sem Dedicada' in df_p.columns:
                fps = df_p['FP sem Dedicada'].sum()
                print(f"      FP sem Dedicada:   R$ {fps:>18,.2f}")

    if erros == 0:
        print(f"\n   ✅ Validação final concluída sem erros")
    else:
        print(f"\n   ⚠️ Validação final: {erros} problema(s) encontrado(s)")

    print("=" * 60)


# ═══════════════════════════════════════════════════════════════
#  ORQUESTRADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def processar_veiculos_real(ano: Optional[int] = None,
                            progress_callback=None) -> Dict:
    """
    Pipeline completo de processamento Real (Sapiens) para TC Principal.

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

    log("🚀 PROCESSAMENTO REAL (SAPIENS) — TC Principal")
    log("=" * 60)

    # 0. Configuração
    log("⚙️ Configurando ambiente...")
    config = configurar_ambiente(ano)
    log(f"   Ano: {config['ANO_ATUAL']}")
    log(f"   Excel: {config['CAMINHO_EXCEL']}")
    log(f"   Saída: {config['PASTA_SAIDA']}")

    # 1. Sapiens → Despesa Primaria
    log("\n📋 Fase 1/18: Leitura aba Sapiens...")
    df_principal = fase1_sapiens(config)

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
