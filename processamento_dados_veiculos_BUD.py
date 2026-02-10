"""
Módulo de Processamento de Dados BUDGET — TC Principal (Planta Principal)
Processa o arquivo 'Reporting veículos.xlsx' seguindo a lógica metodológica
do TC Ext, mas adaptada ao custo de produção de veículos.

Fases:
  1. massa primária - BDG   → Despesa Primaria (melt meses→linhas)
  2. massa - REDIS           → Redis (receita, valor negativo)
  3. Merge Voz + Redis      → tabela principal
  4. Volume e EST PdR - BDG → Vol FA + Tempo FA
  5. Volume BDG / actual    → volumes de veículos
  6. EST veículos - BDG     → merge com volume → Tempo Veic
  7. Rateio FA              → %FA por oficina (automático BS/PS/PL, manual QY/GS/SM)
  8. Custo FA               → Rateio FA × Despesa Primaria
  9. Custo FP               -> Despesa Primaria - Custo FA + Redis
 10. massa - D&A dedicado   → amortizações por modelo/oficina
 11. FP sem Dedicada        → Custo FP − D&A dedicado
 12. Salvamento             → parquets em dados/{ano}/TC_Principal/BUD/
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
#  UTILITÁRIOS  (reaproveitados do processamento_dados_BUD.py)
# ═══════════════════════════════════════════════════════════════

MAPEAMENTO_MESES = {
    'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
    'marco': 'Março', 'abril': 'Abril', 'maio': 'Maio',
    'junho': 'Junho', 'julho': 'Julho', 'agosto': 'Agosto',
    'setembro': 'Setembro', 'outubro': 'Outubro',
    'novembro': 'Novembro', 'dezembro': 'Dezembro',
}

PREFIXOS_MESES = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun',
                  'jul', 'ago', 'set', 'out', 'nov', 'dez']

OFICINAS_RATEIO_AUTOMATICO = ['BS', 'PS', 'PL']
OFICINAS_RATEIO_MANUAL = ['QY', 'GS', 'SM']
# Oficinas cujo Tempo Veíc NÃO entra no denominador da taxa PdR global.
# No Excel (aba Rateio PdR, Row 15), GS e SM são excluídos do total.
OFICINAS_EXCLUIR_DENOM_TAXA_PDR = ['GS', 'SM']


def _normalizar_nome_coluna(col: object) -> str:
    s = '' if col is None else str(col)
    s = s.strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = ''.join(ch for ch in s if ch.isalnum())
    return s


def _corrigir_mojibake(texto: str) -> str:
    """Corrige encoding mojibake comum (mar�o → março, Vari�vel → Variável etc.)"""
    if not isinstance(texto, str):
        return texto
    substituicoes = {
        'mar\ufffd': 'março', 'Mar\ufffd': 'Março',
        'mar�o': 'março', 'Mar�o': 'Março',
        'Vari\ufffd': 'Variá', 'Vari�vel': 'Variável',
        'Ve\ufffd': 'Veí', 'Ve�culo': 'Veículo',
        'Per\ufffd': 'Perí', 'Per�odo': 'Período',
        'S\ufffd': 'Sí', 'S�nt': 'Sínt',
        '\ufffd': 'í',
    }
    for errado, certo in substituicoes.items():
        texto = texto.replace(errado, certo)
    return texto


def _corrigir_colunas_mojibake(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica correção de mojibake nos nomes das colunas."""
    novas = {}
    for c in df.columns:
        corrigida = _corrigir_mojibake(str(c))
        if corrigida != str(c):
            novas[c] = corrigida
    if novas:
        df = df.rename(columns=novas)
    return df


def _detectar_colunas_meses(df: pd.DataFrame) -> list[str]:
    """Detecta colunas cujo nome representa um mês (jan*/fev*/mar*…)."""
    meses_encontrados = []
    for col in df.columns:
        nome = _corrigir_mojibake(str(col)).strip().lower()
        nome_norm = unicodedata.normalize('NFKD', nome)
        nome_norm = ''.join(ch for ch in nome_norm if not unicodedata.combining(ch))
        for pref in PREFIXOS_MESES:
            if nome_norm.startswith(pref):
                meses_encontrados.append(col)
                break
    return meses_encontrados


def _normalizar_periodo(valor: str) -> str:
    """Converte 'janeiro' → 'Janeiro', corrigindo mojibake antes."""
    if not isinstance(valor, str):
        return valor
    val = _corrigir_mojibake(valor).strip()
    val_norm = unicodedata.normalize('NFKD', val).lower()
    val_norm = ''.join(ch for ch in val_norm if not unicodedata.combining(ch))
    for chave, capitalizado in MAPEAMENTO_MESES.items():
        chave_norm = unicodedata.normalize('NFKD', chave)
        chave_norm = ''.join(ch for ch in chave_norm if not unicodedata.combining(ch))
        if val_norm == chave_norm:
            return capitalizado
    return val.capitalize()


def _exigir_colunas(df: pd.DataFrame, obrigatorias: list[str], contexto: str) -> None:
    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        disponiveis = ', '.join([str(c) for c in df.columns[:80]])
        raise ValueError(
            f"❌ Estrutura inesperada em {contexto}. Faltando colunas: {faltando}. "
            f"Colunas disponíveis: {disponiveis}"
        )


def _validar_abas_excel(caminho: str, abas_obrigatorias: list[str]) -> None:
    xl = pd.ExcelFile(caminho)
    abas = xl.sheet_names
    faltando = [a for a in abas_obrigatorias if a not in abas]
    if faltando:
        raise ValueError(
            f"❌ Abas obrigatórias não encontradas: {faltando}. Abas disponíveis: {abas}"
        )


def normalizar_tipos_para_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos de dados para evitar erros ao salvar parquet."""
    df = df.copy()
    colunas_numericas = []
    for col in df.columns:
        try:
            sample = df[col].dropna().head(100)
            if len(sample) > 0:
                pd.to_numeric(sample, errors='raise')
                colunas_numericas.append(col)
        except (ValueError, TypeError):
            pass

    for col in colunas_numericas:
        if df[col].dtype == 'object':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in df.columns:
        if df[col].dtype == 'object' and col not in colunas_numericas:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)

    return df


# ═══════════════════════════════════════════════════════════════
#  CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════

def configurar_ambiente(ano: Optional[int] = None) -> Dict:
    """Configura ano, pastas e valida existência do arquivo Excel."""
    if ano is None:
        ano = datetime.now().year

    pasta_ano = f'dados/TC_Principal/{ano}'
    pasta_saida = f'dados/TC_Principal/{ano}/BUD'
    pasta_historico = 'dados/TC_Principal/historico_consolidado/BUD'

    os.makedirs(pasta_saida, exist_ok=True)
    os.makedirs(pasta_historico, exist_ok=True)

    # Localizar arquivo
    caminho_ano = os.path.join(pasta_ano, 'Reporting veículos.xlsx')
    caminho_raiz = 'Reporting veículos.xlsx'

    if os.path.exists(caminho_ano):
        caminho = caminho_ano
    elif os.path.exists(caminho_raiz):
        shutil.copy2(caminho_raiz, caminho_ano)
        caminho = caminho_ano
        print(f"  📦 Copiado {caminho_raiz} → {caminho_ano}")
    else:
        raise FileNotFoundError(
            f"❌ Arquivo não encontrado em {caminho_ano} nem em {caminho_raiz}"
        )

    # Validar abas obrigatórias
    abas_obrigatorias = [
        'massa primária - BDG', 'massa - REDIS', 'Volume e EST PdR - BDG',
        'Volume BDG', 'Volume actual', 'EST veículos - BDG', 'massa - D&A dedicado'
    ]
    _validar_abas_excel(caminho, abas_obrigatorias)

    # Carregar rateios manuais
    json_path = 'rateios_manuais.json'
    rateios_manuais = {'QY': 0.0, 'GS': 0.0, 'SM': 0.0}
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            dados_json = json.load(f)
        for oficina in OFICINAS_RATEIO_MANUAL:
            if oficina in dados_json:
                rateios_manuais[oficina] = float(dados_json[oficina])
    else:
        print(f"  ⚠️ Arquivo {json_path} não encontrado. Rateios manuais zerados.")

    config = {
        'ANO_ATUAL': ano,
        'PASTA_ANO': pasta_ano,
        'PASTA_SAIDA': pasta_saida,
        'PASTA_HISTORICO': pasta_historico,
        'CAMINHO_EXCEL': caminho,
        'RATEIOS_MANUAIS': rateios_manuais,
    }

    print(f"\n{'='*60}")
    print(f"  TC PRINCIPAL — Processamento Budget {ano}")
    print(f"{'='*60}")
    print(f"  📂 Excel: {caminho}")
    print(f"  📂 Saída: {pasta_saida}")
    print(f"  📊 Rateios manuais: {rateios_manuais}")
    print()

    return config


# ═══════════════════════════════════════════════════════════════
#  FASE 1 — massa primária - BDG → Despesa Primaria
# ═══════════════════════════════════════════════════════════════

def fase1_voz_de_custo(config: Dict) -> pd.DataFrame:
    """Carrega aba 'massa primária - BDG', faz melt meses→linhas."""
    print("── FASE 1: massa primária - BDG → Despesa Primaria ──")

    df = pd.read_excel(config['CAMINHO_EXCEL'], sheet_name='massa primária - BDG')
    df = _corrigir_colunas_mojibake(df)

    # Identificar colunas dimensionais vs meses
    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        raise ValueError("❌ Nenhuma coluna de mês encontrada na aba massa primária - BDG")

    # Excluir coluna Ano (somatório anual)
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    # Melt
    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='Despesa Primaria'
    )

    # Normalizar período
    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)

    # Preencher NaN em valores numéricos com 0
    df_melt['Despesa Primaria'] = df_melt['Despesa Primaria'].fillna(0)

    # Remover linhas sem Oficina (linhas em branco da planilha)
    df_melt = df_melt[df_melt['Oficina'].notna()].copy()

    _exigir_colunas(df_melt, ['Oficina', 'Account', 'Período', 'Despesa Primaria'],
                    'Fase 1 - Voz de custo')

    print(f"  ✅ Shape: {df_melt.shape}")
    print(f"  Oficinas: {sorted(df_melt['Oficina'].dropna().unique())}")
    print(f"  Períodos: {df_melt['Período'].nunique()} meses")
    print(f"  ∑ Despesa Primaria: {df_melt['Despesa Primaria'].sum():,.2f}")
    print()

    return df_melt


# ═══════════════════════════════════════════════════════════════
#  FASE 2 — massa - REDIS → Redis (receita, valor negativo)
# ═══════════════════════════════════════════════════════════════

def fase2_redis(config: Dict) -> pd.DataFrame:
    """Carrega aba 'massa - REDIS', faz melt e inverte sinal (receita)."""
    print("── FASE 2: massa - REDIS → Redis (receita) ──")

    df = pd.read_excel(config['CAMINHO_EXCEL'], sheet_name='massa - REDIS')
    df = _corrigir_colunas_mojibake(df)

    colunas_meses = _detectar_colunas_meses(df)
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='Redis'
    )

    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)
    df_melt['Redis'] = df_melt['Redis'].fillna(0)

    # Inverter sinal: Redis é receita, deve ser negativo
    df_melt['Redis'] = -df_melt['Redis'].abs()

    # Remover linhas sem Oficina
    df_melt = df_melt[df_melt['Oficina'].notna()].copy()

    _exigir_colunas(df_melt, ['Oficina', 'Account', 'Período', 'Redis'],
                    'Fase 2 - massa - REDIS')

    linhas_nonzero = (df_melt['Redis'] != 0).sum()
    print(f"  ✅ Shape: {df_melt.shape}")
    print(f"  Linhas com Redis ≠ 0: {linhas_nonzero}")
    print(f"  ∑ Redis: {df_melt['Redis'].sum():,.2f}")
    print()

    return df_melt


# ═══════════════════════════════════════════════════════════════
#  FASE 3 — Merge Voz de Custo + Redis → Tabela Principal
# ═══════════════════════════════════════════════════════════════

def fase3_merge_voz_redis(df_voz: pd.DataFrame, df_redis: pd.DataFrame) -> pd.DataFrame:
    """Merge LEFT de Voz de custo com Redis por (Oficina, Account, Período)."""
    print("── FASE 3: Merge Voz de Custo + Redis ──")

    chaves = ['Oficina', 'Account', 'Período']

    # Garantir que ambos tenham as mesmas colunas dimensionais para merge
    # Redis deve trazer só as chaves + coluna Redis
    colunas_redis_merge = chaves + ['Redis']
    # Verificar se colunas adicionais na redis (Type 05, Type 06, Custo) são iguais
    # Se sim, podemos adicionar ao merge para evitar duplicação
    colunas_dim_comuns = [c for c in df_redis.columns
                         if c not in ['Redis', 'Período'] and c in df_voz.columns and c not in chaves]
    colunas_redis_para_merge = chaves + ['Redis']

    # Agregar Redis pela chave para evitar duplicação one-to-many
    df_redis_agg = df_redis.groupby(chaves, as_index=False)['Redis'].sum()

    count_antes = len(df_voz)

    df_principal = pd.merge(
        df_voz,
        df_redis_agg,
        on=chaves,
        how='left'
    )

    df_principal['Redis'] = df_principal['Redis'].fillna(0)

    count_depois = len(df_principal)

    print(f"  ✅ Shape: {df_principal.shape}")
    print(f"  Linhas antes: {count_antes} → depois: {count_depois} (delta: {count_depois - count_antes})")
    if count_depois != count_antes:
        print(f"  ⚠️ ATENÇÃO: Houve expansão de linhas no merge!")
    print(f"  ∑ Despesa Primaria: {df_principal['Despesa Primaria'].sum():,.2f}")
    print(f"  ∑ Redis: {df_principal['Redis'].sum():,.2f}")
    print()

    return df_principal


# ═══════════════════════════════════════════════════════════════
#  FASE 4 — Volume e EST PdR → Vol FA + Tempo FA
# ═══════════════════════════════════════════════════════════════

def fase4_volume_est_fa(config: Dict) -> pd.DataFrame:
    """Carrega 'Volume e EST PdR - BDG', melt meses→linhas, calcula Tempo FA."""
    print("── FASE 4: Volume e EST PdR - BDG → Vol FA + Tempo FA ──")

    # Ler raw e encontrar a linha do cabeçalho real
    df_raw = pd.read_excel(config['CAMINHO_EXCEL'],
                           sheet_name='Volume e EST PdR - BDG', header=None)

    # Encontrar a linha que contém 'REF FER' ou 'Oficina'
    header_row = None
    for i in range(min(10, len(df_raw))):
        row_vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v)]
        if any('Oficina' in v for v in row_vals) or any('REF' in v for v in row_vals):
            header_row = i
            break

    if header_row is None:
        raise ValueError("❌ Não encontrei cabeçalho na aba 'Volume e EST PdR - BDG'")

    # Reler com header correto
    df = pd.read_excel(config['CAMINHO_EXCEL'],
                       sheet_name='Volume e EST PdR - BDG',
                       header=header_row)
    df = _corrigir_colunas_mojibake(df)

    # Renomear colunas se necessário
    colunas_rename = {}
    for c in df.columns:
        c_lower = str(c).strip().lower()
        if 'ref' in c_lower:
            colunas_rename[c] = 'REF FER'
        elif 'oficina' in c_lower:
            colunas_rename[c] = 'Oficina'
        elif c_lower == 'est':
            colunas_rename[c] = 'EST'
    if colunas_rename:
        df = df.rename(columns=colunas_rename)

    _exigir_colunas(df, ['Oficina', 'EST'], 'Fase 4 - Volume e EST PdR')

    # Remover coluna Ano se existir
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # Detectar colunas de meses
    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        raise ValueError("❌ Nenhuma coluna de mês encontrada na aba Volume e EST PdR")

    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    # Converter EST para numérico
    df['EST'] = pd.to_numeric(df['EST'], errors='coerce').fillna(0)

    # Melt
    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='Vol FA'
    )

    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)
    df_melt['Vol FA'] = pd.to_numeric(df_melt['Vol FA'], errors='coerce').fillna(0)

    # Calcular Tempo FA = Vol FA * EST
    df_melt['Tempo FA'] = df_melt['Vol FA'] * df_melt['EST']

    # Remover linhas sem oficina
    df_melt = df_melt[df_melt['Oficina'].notna()].copy()

    print(f"  ✅ Shape: {df_melt.shape}")
    print(f"  Oficinas: {sorted(df_melt['Oficina'].dropna().unique())}")
    print(f"  Nº peças (REF FER): {df_melt['REF FER'].nunique() if 'REF FER' in df_melt.columns else 'N/A'}")
    print(f"  ∑ Vol FA: {df_melt['Vol FA'].sum():,.0f}")
    print(f"  ∑ Tempo FA: {df_melt['Tempo FA'].sum():,.2f}")
    print()

    return df_melt


# ═══════════════════════════════════════════════════════════════
#  FASE 5 — Volume BDG + Volume actual → volumes veículos
# ═══════════════════════════════════════════════════════════════

def _ler_volume_veiculos(config: Dict, sheet_name: str, label: str) -> pd.DataFrame:
    """Lê uma aba de volume de veículos (BDG ou actual), excluindo a primeira linha
    (que contém cabeçalhos fora de posição), e faz melt."""
    df = pd.read_excel(config['CAMINHO_EXCEL'], sheet_name=sheet_name, header=1)
    df = _corrigir_colunas_mojibake(df)

    # A primeira coluna é o nome do veículo (pode ter nome Unnamed:0)
    primeira_col = df.columns[0]
    df = df.rename(columns={primeira_col: 'Veículo'})

    # Remover linha Total se existir
    df = df[~df['Veículo'].astype(str).str.strip().str.lower().isin(['total', 'nan', ''])].copy()
    df = df[df['Veículo'].notna()].copy()

    # Remover coluna Ano
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    colunas_meses = _detectar_colunas_meses(df)
    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='Volume'
    )

    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)
    df_melt['Volume'] = pd.to_numeric(df_melt['Volume'], errors='coerce').fillna(0)

    print(f"  ✅ {label} — Shape: {df_melt.shape}")
    print(f"  Veículos: {sorted(df_melt['Veículo'].dropna().unique())}")
    print(f"  ∑ Volume: {df_melt['Volume'].sum():,.0f}")

    return df_melt


def fase5_volumes_veiculos(config: Dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carrega Volume BDG e Volume actual."""
    print("── FASE 5: Volumes de Veículos (BDG + Actual) ──")

    df_vol_bud = _ler_volume_veiculos(config, 'Volume BDG', 'Volume BDG')
    df_vol_actual = _ler_volume_veiculos(config, 'Volume actual', 'Volume Actual')

    print()
    return df_vol_bud, df_vol_actual


# ═══════════════════════════════════════════════════════════════
#  FASE 6 — EST veículos + Volume → Tempo Veículo
# ═══════════════════════════════════════════════════════════════

def fase6_tempo_veiculo(config: Dict, df_vol_bud: pd.DataFrame) -> pd.DataFrame:
    """Carrega EST veículos - BDG e faz merge com volume para calcular Tempo Veic."""
    print("── FASE 6: EST veículos + Volume → Tempo Veículo ──")

    df = pd.read_excel(config['CAMINHO_EXCEL'],
                       sheet_name='EST veículos - BDG', header=1)
    df = _corrigir_colunas_mojibake(df)

    # Corrigir nome coluna Veículo se estiver com mojibake
    renames = {}
    for c in df.columns:
        c_corrigido = _corrigir_mojibake(str(c))
        if c_corrigido != str(c):
            renames[c] = c_corrigido
    if renames:
        df = df.rename(columns=renames)

    # Manter somente Oficina, Veículo, EST (as 3 primeiras colunas relevantes)
    _exigir_colunas(df, ['Oficina', 'EST'], 'Fase 6 - EST veículos')

    # Coluna Veículo pode estar como 'Veículo' ou col[1]
    col_veiculo = None
    for c in df.columns:
        if 've' in str(c).lower() and 'cul' in str(c).lower():
            col_veiculo = c
            break
    if col_veiculo is None:
        col_veiculo = df.columns[1]

    if col_veiculo != 'Veículo':
        df = df.rename(columns={col_veiculo: 'Veículo'})

    # Manter somente as 3 colunas essenciais
    df_est = df[['Oficina', 'Veículo', 'EST']].copy()
    df_est = df_est[df_est['Oficina'].notna() & df_est['Veículo'].notna()].copy()
    df_est['EST'] = pd.to_numeric(df_est['EST'], errors='coerce').fillna(0)

    print(f"  EST veículos carregado: {df_est.shape[0]} linhas")
    print(f"  Oficinas: {sorted(df_est['Oficina'].dropna().unique())}")
    print(f"  Veículos: {sorted(df_est['Veículo'].dropna().unique())}")

    # Merge com volume BDG
    # Volume BDG não tem coluna Oficina — verificar
    if 'Oficina' not in df_vol_bud.columns:
        # Volume BDG é por Veículo apenas; merge só por Veículo
        print("  ℹ️ Volume BDG não tem Oficina. Merge por Veículo apenas.")
        df_tempo = pd.merge(df_est, df_vol_bud[['Veículo', 'Período', 'Volume']],
                            on=['Veículo'], how='left')
    else:
        df_tempo = pd.merge(df_vol_bud, df_est,
                            on=['Oficina', 'Veículo'], how='left')

    df_tempo['Volume'] = df_tempo['Volume'].fillna(0)
    df_tempo['EST'] = df_tempo['EST'].fillna(0)

    # Calcular Tempo Veículo
    df_tempo['Tempo Veic'] = df_tempo['Volume'] * df_tempo['EST']

    print(f"  ✅ Tempo Veículo calculado — Shape: {df_tempo.shape}")
    print(f"  ∑ Volume: {df_tempo['Volume'].sum():,.0f}")
    print(f"  ∑ Tempo Veic: {df_tempo['Tempo Veic'].sum():,.2f}")
    print()

    return df_tempo


# ═══════════════════════════════════════════════════════════════
#  FASE 7 — Rateio FA
# ═══════════════════════════════════════════════════════════════

def fase7_rateio_fa(config: Dict, df_fa: pd.DataFrame,
                    df_tempo_veic: pd.DataFrame) -> pd.DataFrame:
    """Calcula Rateio FA: %FA = ∑Tempo FA / (∑Tempo FA + ∑Tempo Veic) por oficina/período.
    Para QY, GS, SM usa fatores × taxa global de produção PdR."""
    print("── FASE 7: Rateio FA ──")

    rateios_manuais = config['RATEIOS_MANUAIS']

    # Agregar Tempo FA por oficina e período
    tempo_fa_agg = (df_fa.groupby(['Oficina', 'Período'], as_index=False)['Tempo FA']
                    .sum().rename(columns={'Tempo FA': 'Tempo FA Total'}))

    # Agregar Tempo Veic por oficina e período
    tempo_veic_agg = (df_tempo_veic.groupby(['Oficina', 'Período'], as_index=False)['Tempo Veic']
                      .sum().rename(columns={'Tempo Veic': 'Tempo Veic Total'}))

    # Merge tempos
    df_rateio = pd.merge(tempo_fa_agg, tempo_veic_agg, on=['Oficina', 'Período'], how='outer')
    df_rateio['Tempo FA Total'] = df_rateio['Tempo FA Total'].fillna(0)
    df_rateio['Tempo Veic Total'] = df_rateio['Tempo Veic Total'].fillna(0)

    # ── Rateio automático para BS, PS, PL ──
    # Fórmula correta: %FA = Tempo FA / (Tempo FA + Tempo Veíc)
    df_rateio['Rateio FA'] = 0.0

    mask_auto = df_rateio['Oficina'].isin(OFICINAS_RATEIO_AUTOMATICO)
    denominador = (df_rateio.loc[mask_auto, 'Tempo FA Total']
                   + df_rateio.loc[mask_auto, 'Tempo Veic Total'])
    df_rateio.loc[mask_auto, 'Rateio FA'] = np.where(
        denominador > 0,
        df_rateio.loc[mask_auto, 'Tempo FA Total'] / denominador,
        0.0
    )

    # ── Rateio manual para QY, GS, SM ──
    # Fórmula: %PdR = fator × (∑Tempo FA total / ∑Tempo Veíc total)
    # O denominador exclui GS e SM conforme a planilha Excel
    # (aba Rateio PdR, Row 15 = Total min veíc sem GS/SM)
    tfa_total_periodo = (df_fa.groupby('Período', as_index=False)['Tempo FA']
                         .sum().rename(columns={'Tempo FA': 'TFA_global'}))
    # Filtrar: excluir oficinas que NÃO entram no denominador
    df_tvc_filtrado = df_tempo_veic[
        ~df_tempo_veic['Oficina'].isin(OFICINAS_EXCLUIR_DENOM_TAXA_PDR)
    ]
    tvc_total_periodo = (df_tvc_filtrado.groupby('Período', as_index=False)['Tempo Veic']
                         .sum().rename(columns={'Tempo Veic': 'TVC_global'}))
    taxa_prod = pd.merge(tfa_total_periodo, tvc_total_periodo, on='Período')
    taxa_prod['taxa_pdr'] = np.where(
        taxa_prod['TVC_global'] > 0,
        taxa_prod['TFA_global'] / taxa_prod['TVC_global'],
        0.0
    )
    print(f"  Taxa PdR global (média): {taxa_prod['taxa_pdr'].mean():.6f}")
    print(f"  Oficinas excluídas do denominador: {OFICINAS_EXCLUIR_DENOM_TAXA_PDR}")

    for oficina, fator in rateios_manuais.items():
        mask = df_rateio['Oficina'] == oficina
        if mask.any():
            # Oficina já existe no df_rateio (tem Tempo Veíc)
            for _, row_taxa in taxa_prod.iterrows():
                mask_per = mask & (df_rateio['Período'] == row_taxa['Período'])
                df_rateio.loc[mask_per, 'Rateio FA'] = fator * row_taxa['taxa_pdr']
        else:
            # Oficina não apareceu — criar linhas
            novas = pd.DataFrame({
                'Oficina': oficina,
                'Período': taxa_prod['Período'],
                'Tempo FA Total': 0.0,
                'Tempo Veic Total': 0.0,
                'Rateio FA': fator * taxa_prod['taxa_pdr'].values,
            })
            df_rateio = pd.concat([df_rateio, novas], ignore_index=True)

        if fator == 0.0:
            print(f"  ⚠️ Fator manual para {oficina} = 0.0 — preencha em rateios_manuais.json")

    # Verificar oficinas que não apareceram em nenhum dos dataframes
    oficinas_rateio = set(df_rateio['Oficina'].unique())
    for of_manual in OFICINAS_RATEIO_MANUAL:
        if of_manual not in oficinas_rateio:
            novas = pd.DataFrame({
                'Oficina': of_manual,
                'Período': taxa_prod['Período'],
                'Tempo FA Total': 0.0,
                'Tempo Veic Total': 0.0,
                'Rateio FA': rateios_manuais.get(of_manual, 0.0) * taxa_prod['taxa_pdr'].values,
            })
            df_rateio = pd.concat([df_rateio, novas], ignore_index=True)

    # Validação
    rateio_max = df_rateio['Rateio FA'].max()
    if rateio_max > 1.0:
        print(f"  ⚠️ ATENÇÃO: Rateio FA máximo = {rateio_max:.4f} (> 100%)")
    if rateio_max > 0.5:
        print(f"  ℹ️ Rateio FA máximo = {rateio_max:.4f} — verificar se faz sentido")

    print(f"  ✅ Rateio FA calculado — Shape: {df_rateio.shape}")
    print(f"  Rateio por oficina (média):")
    for of in sorted(df_rateio['Oficina'].unique()):
        media = df_rateio.loc[df_rateio['Oficina'] == of, 'Rateio FA'].mean()
        tipo = 'automático' if of in OFICINAS_RATEIO_AUTOMATICO else 'manual'
        print(f"    {of}: {media:.4f} ({media*100:.2f}%) [{tipo}]")
    print()

    return df_rateio[['Oficina', 'Período', 'Rateio FA']]


# ═══════════════════════════════════════════════════════════════
#  FASE 8 — Custo FA = Rateio FA × Despesa Primaria
# ═══════════════════════════════════════════════════════════════

def fase8_custo_fa(df_principal: pd.DataFrame, df_rateio: pd.DataFrame) -> pd.DataFrame:
    """Traz Rateio FA para a tabela principal e calcula Custo FA."""
    print("── FASE 8: Custo FA ──")

    count_antes = len(df_principal)

    df_principal = pd.merge(
        df_principal,
        df_rateio,
        on=['Oficina', 'Período'],
        how='left'
    )

    df_principal['Rateio FA'] = df_principal['Rateio FA'].fillna(0)
    df_principal['Custo FA'] = df_principal['Rateio FA'] * df_principal['Despesa Primaria']

    count_depois = len(df_principal)
    if count_depois != count_antes:
        print(f"  ⚠️ Expansão de linhas: {count_antes} → {count_depois}")

    print(f"  ✅ Shape: {df_principal.shape}")
    print(f"  ∑ Custo FA: {df_principal['Custo FA'].sum():,.2f}")
    print()

    return df_principal


# ═══════════════════════════════════════════════════════════════
#  FASE 9 — Custo FP = Despesa Primaria − Custo FA + Redis
# ═══════════════════════════════════════════════════════════════

def fase9_custo_fp(df_principal: pd.DataFrame) -> pd.DataFrame:
    """Calcula Custo FP (Fabricação Principal).
    Custo FP = Despesa Primaria - Custo FA + Redis
    Nota: Redis já é negativo (receita), então somar Redis subtrai seu valor
    absoluto do custo.
    """
    print("── FASE 9: Custo FP ──")

    df_principal['Custo FP'] = (
        df_principal['Despesa Primaria']
        - df_principal['Custo FA']
        + df_principal['Redis']
    )

    negativos = (df_principal['Custo FP'] < 0).sum()
    if negativos > 0:
        print(f"  ℹ️ {negativos} linhas com Custo FP negativo")

    # Prova cruzada: DP - FA + Redis - FP deve ser ~0
    diff = (df_principal['Despesa Primaria']
            - df_principal['Custo FA']
            + df_principal['Redis']
            - df_principal['Custo FP']).abs().sum()
    print(f"  📐 Prova cruzada (deve ser ~0): {diff:.6f}")

    print(f"  ✅ Shape: {df_principal.shape}")
    print(f"  ∑ Despesa Primaria: {df_principal['Despesa Primaria'].sum():,.2f}")
    print(f"  ∑ Custo FA: {df_principal['Custo FA'].sum():,.2f}")
    print(f"  ∑ Redis: {df_principal['Redis'].sum():,.2f}")
    print(f"  ∑ Custo FP: {df_principal['Custo FP'].sum():,.2f}")
    print()

    return df_principal


# ═══════════════════════════════════════════════════════════════
#  FASE 10 — D&A Dedicado
# ═══════════════════════════════════════════════════════════════

def fase10_dea_dedicado(config: Dict) -> pd.DataFrame:
    """Carrega aba 'massa - D&A dedicado', excluir Ano, melt meses→linhas."""
    print("── FASE 10: massa - D&A Dedicado ──")

    df = pd.read_excel(config['CAMINHO_EXCEL'], sheet_name='massa - D&A dedicado')
    df = _corrigir_colunas_mojibake(df)

    # Corrigir coluna Veículo
    renames = {}
    for c in df.columns:
        c_corrigido = _corrigir_mojibake(str(c))
        if c_corrigido != str(c):
            renames[c] = c_corrigido
    if renames:
        df = df.rename(columns=renames)

    # Remover Ano
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # Remover linhas vazias (ex: linha separadora entre BS e PS)
    df = df[df['Oficina'].notna()].copy()

    colunas_meses = _detectar_colunas_meses(df)
    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='D&A dedicado'
    )

    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)
    df_melt['D&A dedicado'] = df_melt['D&A dedicado'].fillna(0)

    # Verificar coluna Veículo
    col_veiculo = None
    for c in df_melt.columns:
        if 'veículo' in str(c).lower() or 'veiculo' in str(c).lower():
            col_veiculo = c
            break
    if col_veiculo and col_veiculo != 'Veículo':
        df_melt = df_melt.rename(columns={col_veiculo: 'Veículo'})

    print(f"  ✅ Shape: {df_melt.shape}")
    print(f"  Oficinas: {sorted(df_melt['Oficina'].dropna().unique())}")
    if 'Veículo' in df_melt.columns:
        print(f"  Veículos: {sorted(df_melt['Veículo'].dropna().unique())}")
    print(f"  Accounts: {sorted(df_melt['Account'].dropna().unique())}")
    print(f"  ∑ D&A dedicado: {df_melt['D&A dedicado'].sum():,.2f}")
    print()

    return df_melt


# ═══════════════════════════════════════════════════════════════
#  FASE 11 — Merge D&A + FP sem Dedicada
# ═══════════════════════════════════════════════════════════════

def fase11_fp_sem_dedicada(df_principal: pd.DataFrame,
                           df_dea: pd.DataFrame) -> pd.DataFrame:
    """Merge D&A dedicado → tabela principal. Calcula FP sem Dedicada."""
    print("── FASE 11: Merge D&A + FP sem Dedicada ──")

    # D&A tem Veículo, mas tabela principal não tem.
    # Agregar D&A por Oficina + Account + Período (somar todos os veículos)
    chaves = ['Oficina', 'Account', 'Período']
    df_dea_agg = df_dea.groupby(chaves, as_index=False)['D&A dedicado'].sum()

    count_antes = len(df_principal)

    df_principal = pd.merge(
        df_principal,
        df_dea_agg,
        on=chaves,
        how='left'
    )

    df_principal['D&A dedicado'] = df_principal['D&A dedicado'].fillna(0)
    df_principal['FP sem Dedicada'] = df_principal['Custo FP'] - df_principal['D&A dedicado']

    count_depois = len(df_principal)
    if count_depois != count_antes:
        print(f"  ⚠️ Expansão de linhas: {count_antes} → {count_depois}")

    print(f"  ✅ Shape: {df_principal.shape}")
    print(f"  ∑ D&A dedicado: {df_principal['D&A dedicado'].sum():,.2f}")
    print(f"  ∑ Custo FP: {df_principal['Custo FP'].sum():,.2f}")
    print(f"  ∑ FP sem Dedicada: {df_principal['FP sem Dedicada'].sum():,.2f}")
    print()

    return df_principal


# ═══════════════════════════════════════════════════════════════
#  FASE 12 — Salvamento
# ═══════════════════════════════════════════════════════════════

def fase12_salvamento(config: Dict,
                      df_principal: pd.DataFrame,
                      df_fa: pd.DataFrame,
                      df_tempo_veic: pd.DataFrame,
                      df_vol_bud: pd.DataFrame,
                      df_vol_actual: pd.DataFrame,
                      df_dea: pd.DataFrame) -> Dict[str, str]:
    """Salva todos os DataFrames em parquet."""
    print("── FASE 12: Salvamento ──")

    ano = config['ANO_ATUAL']
    pasta = config['PASTA_SAIDA']

    # Adicionar coluna Ano a todos
    dfs_para_salvar = {
        'df_principal_BUD.parquet': df_principal,
        'df_volume_fa_BUD.parquet': df_fa,
        'df_tempo_veiculos_BUD.parquet': df_tempo_veic,
        'df_vol_veiculos_BUD.parquet': df_vol_bud,
        'df_vol_veiculos_actual.parquet': df_vol_actual,
        'df_dea_dedicado_BUD.parquet': df_dea,
    }

    arquivos_salvos = {}

    for nome, df in dfs_para_salvar.items():
        df = df.copy()
        if 'Ano' not in df.columns:
            df['Ano'] = ano
        df = normalizar_tipos_para_parquet(df)
        caminho = os.path.join(pasta, nome)
        df.to_parquet(caminho, index=False)
        arquivos_salvos[nome] = caminho
        print(f"  💾 {nome} — {df.shape[0]} linhas × {df.shape[1]} colunas → {caminho}")

    print()
    return arquivos_salvos


# ═══════════════════════════════════════════════════════════════
#  VALIDAÇÃO FINAL
# ═══════════════════════════════════════════════════════════════

def validacao_final(config: Dict, arquivos: Dict[str, str]) -> None:
    """Verifica integridade dos arquivos salvos."""
    print("── VALIDAÇÃO FINAL ──")

    erros = []
    for nome, caminho in arquivos.items():
        if not os.path.exists(caminho):
            erros.append(f"  ❌ {nome} — arquivo não encontrado!")
            continue

        df = pd.read_parquet(caminho)
        nulos_total = df.isnull().sum().sum()

        # Verificar Ano
        if 'Ano' not in df.columns:
            erros.append(f"  ❌ {nome} — coluna 'Ano' ausente")
        elif df['Ano'].nunique() != 1:
            erros.append(f"  ❌ {nome} — múltiplos anos: {df['Ano'].unique()}")

        # Verificar períodos
        if 'Período' in df.columns:
            n_periodos = df['Período'].nunique()
            if n_periodos != 12:
                erros.append(f"  ⚠️ {nome} — {n_periodos} períodos (esperado 12)")

        print(f"  ✅ {nome} — {df.shape} — NaN total: {nulos_total}")

    if erros:
        print(f"\n  ⚠️ {len(erros)} problemas encontrados:")
        for e in erros:
            print(e)
    else:
        print(f"\n  🎉 Todos os {len(arquivos)} arquivos validados com sucesso!")

    # Prova cruzada na tabela principal
    try:
        df_p = pd.read_parquet(arquivos['df_principal_BUD.parquet'])
        soma_dp = df_p['Despesa Primaria'].sum()
        soma_fa = df_p['Custo FA'].sum()
        soma_redis = df_p['Redis'].sum()
        soma_fp = df_p['Custo FP'].sum()
        check = soma_dp - soma_fa + soma_redis - soma_fp
        print(f"\n  📐 Prova cruzada: DP - FA + Redis - FP = {check:.6f} (deve ser ~0)")
        print(f"     ∑ Despesa Primaria: {soma_dp:>20,.2f}")
        print(f"     ∑ Custo FA:         {soma_fa:>20,.2f}")
        print(f"     ∑ Redis:            {soma_redis:>20,.2f}")
        print(f"     ∑ Custo FP:         {soma_fp:>20,.2f}")
        print(f"     ∑ FP sem Dedicada:  {df_p['FP sem Dedicada'].sum():>20,.2f}")
    except Exception as e:
        print(f"  ⚠️ Erro na prova cruzada: {e}")

    print()


# ═══════════════════════════════════════════════════════════════
#  ORQUESTRADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def processar_veiculos_budget(ano: Optional[int] = None) -> Dict:
    """Executa todas as fases do processamento do TC Principal (Budget)."""
    inicio = datetime.now()

    # Configuração
    config = configurar_ambiente(ano)

    # Fase 1: Voz de custo → Despesa Primaria
    df_voz = fase1_voz_de_custo(config)

    # Fase 2: REDIS → receita (negativo)
    df_redis = fase2_redis(config)

    # Fase 3: Merge Voz + Redis → tabela principal
    df_principal = fase3_merge_voz_redis(df_voz, df_redis)

    # Fase 4: Volume e EST PdR → Tempo FA
    df_fa = fase4_volume_est_fa(config)

    # Fase 5: Volumes veículos (BDG + actual)
    df_vol_bud, df_vol_actual = fase5_volumes_veiculos(config)

    # Fase 6: EST veículos + Volume → Tempo Veículo
    df_tempo_veic = fase6_tempo_veiculo(config, df_vol_bud)

    # Fase 7: Rateio FA
    df_rateio_fa = fase7_rateio_fa(config, df_fa, df_tempo_veic)

    # Fase 8: Custo FA
    df_principal = fase8_custo_fa(df_principal, df_rateio_fa)

    # Fase 9: Custo FP
    df_principal = fase9_custo_fp(df_principal)

    # Fase 10: D&A dedicado
    df_dea = fase10_dea_dedicado(config)

    # Fase 11: FP sem Dedicada
    df_principal = fase11_fp_sem_dedicada(df_principal, df_dea)

    # Fase 12: Salvamento
    arquivos = fase12_salvamento(
        config, df_principal, df_fa, df_tempo_veic,
        df_vol_bud, df_vol_actual, df_dea
    )

    # Validação
    validacao_final(config, arquivos)

    duracao = datetime.now() - inicio
    print(f"⏱️ Processamento concluído em {duracao.total_seconds():.1f}s")
    print(f"{'='*60}\n")

    return {
        'config': config,
        'arquivos': arquivos,
        'df_principal': df_principal,
        'df_fa': df_fa,
        'df_tempo_veiculos': df_tempo_veic,
        'df_vol_bud': df_vol_bud,
        'df_vol_actual': df_vol_actual,
        'df_dea_dedicado': df_dea,
    }


# ═══════════════════════════════════════════════════════════════
#  EXECUÇÃO DIRETA
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    resultado = processar_veiculos_budget(ano=2026)
