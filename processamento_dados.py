"""
Módulo de Processamento de Dados REAIS
Convertido do notebook dados.ipynb mantendo toda a lógica original
"""

import sys as _sys
import pandas as pd
import numpy as np
import os
import shutil
from datetime import datetime
from typing import Tuple, Dict, Optional
import unicodedata

from tc_core.utils.portabilidade import get_base_path, get_data_root

_ROOT = str(get_base_path())
_DATA_ROOT = os.path.join(str(get_data_root()), 'TC_Ext')


MAPEAMENTO_MESES = {
    'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
    'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
    'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
    'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
}

# Prefixos para identificação de meses mesmo com variações/encoding (ex.: "mar�o")
MAPEAMENTO_MESES_PREFIXO = {
    'jan': 'Janeiro',
    'fev': 'Fevereiro',
    'mar': 'Março',
    'abr': 'Abril',
    'mai': 'Maio',
    'jun': 'Junho',
    'jul': 'Julho',
    'ago': 'Agosto',
    'set': 'Setembro',
    'out': 'Outubro',
    'nov': 'Novembro',
    'dez': 'Dezembro',
}


def _mes_prefixo_de_coluna(col: object) -> str | None:
    norm = _normalizar_nome_coluna(col)
    if not norm:
        return None
    pref = norm[:3]
    return pref if pref in MAPEAMENTO_MESES_PREFIXO else None


def _encontrar_colunas_meses(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return []
    cols = []
    for c in df.columns:
        if _mes_prefixo_de_coluna(c):
            cols.append(c)
    return cols


def _normalizar_nome_mes(mes: object) -> str:
    pref = _mes_prefixo_de_coluna(mes)
    if pref:
        return MAPEAMENTO_MESES_PREFIXO[pref]
    # fallback: tentar lógica antiga por substituição
    s = '' if mes is None else str(mes)
    s = s.strip()
    s_low = s.lower()
    for mes_min, mes_cap in MAPEAMENTO_MESES.items():
        if mes_min in s_low:
            return mes_cap
    return s.strip().capitalize() if s else ''


def _detectar_header_por_rotulos(caminho: str, sheet_name: str, rotulos: list[str], max_linhas: int = 80) -> int | None:
    try:
        df_preview = pd.read_excel(caminho, sheet_name=sheet_name, header=None, nrows=max_linhas)
    except Exception:
        return None

    rot_norm = {_normalizar_nome_coluna(r) for r in rotulos}
    for i in range(len(df_preview)):
        row_vals = df_preview.iloc[i].tolist()
        row_norm = {_normalizar_nome_coluna(v) for v in row_vals if v is not None and str(v).strip()}
        if rot_norm.issubset(row_norm):
            return int(i)
    return None


def _ler_aba_volume_auto(caminho: str, sheet_name: str) -> pd.DataFrame:
    """Lê aba de Volume suportando layout antigo (header=50) e novo (header=0/linha detectada)."""
    # 1) tentativa: layout antigo
    for header in (50, 0, 1, 2):
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=header)
        except ValueError:
            continue
        except Exception:
            continue

        df = limpar_colunas_duplicadas(df)
        df = _aplicar_alias_colunas(
            df,
            {
                'Veículo': ['Veículo', 'Veiculo', 'Veculo'],
                'Oficina': ['Oficina'],
            },
        )
        # se encontrou meses + oficina, consideramos válido
        if 'Oficina' in df.columns and _encontrar_colunas_meses(df):
            return df

    # 2) tentativa: detectar header por rótulos
    header_detectado = _detectar_header_por_rotulos(caminho, sheet_name, ['Oficina', 'Veículo'])
    if header_detectado is None:
        header_detectado = _detectar_header_por_rotulos(caminho, sheet_name, ['Oficina'])
    if header_detectado is not None:
        df = pd.read_excel(caminho, sheet_name=sheet_name, header=header_detectado)
        df = limpar_colunas_duplicadas(df)
        df = _aplicar_alias_colunas(
            df,
            {
                'Veículo': ['Veículo', 'Veiculo', 'Veculo'],
                'Oficina': ['Oficina'],
            },
        )
        return df

    # fallback final: manter erro original para facilitar diagnóstico
    return pd.read_excel(caminho, sheet_name=sheet_name, header=50)


def _normalizar_nome_coluna(col: object) -> str:
    s = '' if col is None else str(col)
    s = s.strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = ''.join(ch for ch in s if ch.isalnum())
    return s


def _aplicar_alias_colunas(df: pd.DataFrame, aliases: Dict[str, list[str]]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    mapeamento = {}
    colunas_norm = {_normalizar_nome_coluna(c): c for c in df.columns}
    for desejada, alternativas in aliases.items():
        if desejada in df.columns:
            continue
        for alt in alternativas:
            col_real = colunas_norm.get(_normalizar_nome_coluna(alt))
            if col_real is not None:
                mapeamento[col_real] = desejada
                break
    if mapeamento:
        df = df.rename(columns=mapeamento)
    return df


def _exigir_colunas(df: pd.DataFrame, obrigatorias: list[str], contexto: str) -> None:
    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        disponiveis = ', '.join([str(c) for c in df.columns[:80]])
        raise ValueError(
            f"❌ Estrutura inesperada em {contexto}. Faltando colunas: {faltando}. "
            f"Colunas disponíveis (parcial): {disponiveis}"
        )


def _validar_abas_excel(caminho: str, abas_obrigatorias: list[str], contexto: str) -> None:
    try:
        xl = pd.ExcelFile(caminho)
        abas = xl.sheet_names
    except Exception as e:
        raise ValueError(f"❌ Não foi possível abrir o Excel ({contexto}): {caminho}. Erro: {e}")

    faltando = [a for a in abas_obrigatorias if a not in abas]
    if faltando:
        raise ValueError(
            f"❌ Abas obrigatórias não encontradas em {contexto}: {faltando}. "
            f"Abas disponíveis: {abas}"
        )


def normalizar_coluna_periodo(df: pd.DataFrame, coluna: str = 'Período') -> pd.DataFrame:
    """Normaliza valores de mês na coluna de período para o formato capitalizado.

    Isso evita falhas de merge entre fontes que trazem meses como "janeiro" vs "Janeiro".
    """
    if df is None or df.empty or coluna not in df.columns:
        return df

    df = df.copy()
    serie = df[coluna]
    if pd.api.types.is_categorical_dtype(serie):
        serie = serie.astype(str)

    serie = serie.astype(str).str.strip()
    for mes_min, mes_cap in MAPEAMENTO_MESES.items():
        serie = serie.str.replace(mes_min, mes_cap, case=False, regex=False)

    df[coluna] = serie
    return df


def limpar_colunas_duplicadas(df):
    """
    Remove colunas duplicadas de forma definitiva:
    1. Remove colunas com sufixos .1, .2, .3, etc (duplicadas pelo pandas)
    2. Remove colunas Unnamed: (vazias do Excel)
    3. Garante que apenas as colunas originais permaneçam
    """
    if df is None or df.empty:
        return df
    
    df = df.copy()
    colunas_para_manter = []
    colunas_ja_vistas = set()
    colunas_removidas = []
    
    for col in df.columns:
        col_str = str(col)
        
        # 1. Remover colunas Unnamed: (vazias do Excel)
        if 'Unnamed:' in col_str or 'unnamed:' in col_str.lower():
            colunas_removidas.append(col_str)
            continue
        
        # 2. Verificar se é coluna duplicada com sufixo numérico (.1, .2, etc)
        if '.' in col_str:
            partes = col_str.rsplit('.', 1)  # Split da direita para a esquerda, apenas 1 vez
            if len(partes) == 2 and partes[1].isdigit():
                # É uma coluna duplicada (ex: "Abril.1", "Janeiro.2")
                colunas_removidas.append(col_str)
                continue
        
        # 3. Verificar se já vimos esta coluna
        if col_str in colunas_ja_vistas:
            colunas_removidas.append(col_str)
            continue
        
        # Adicionar coluna à lista de colunas válidas
        colunas_para_manter.append(col)
        colunas_ja_vistas.add(col_str)
    
    # Retornar DataFrame apenas com colunas válidas
    df_limpo = df[colunas_para_manter].copy()
    
    return df_limpo


def normalizar_tipos_para_parquet(df):
    """Normaliza tipos de dados para evitar erros ao salvar parquet.
    Converte colunas object com tipos mistos (strings e números) para string.
    🔧 CORREÇÃO CRÍTICA: Preserva colunas numéricas importantes (Volume, Total, Valor, etc.)
    """
    df = df.copy()
    
    # Colunas que SEMPRE devem ser string (podem vir como serial numérico do Excel)
    _COLUNAS_SEMPRE_STRING = ['Dt.lçto.', 'Nºdoc.ref.', 'Doc.compra', 'Nºdoc.ref']
    for _cs in _COLUNAS_SEMPRE_STRING:
        if _cs in df.columns:
            df[_cs] = df[_cs].apply(lambda x: str(x) if pd.notna(x) else None)
    
    # 🔧 CORREÇÃO CRÍTICA: Lista de colunas numéricas que NUNCA devem ser convertidas para string
    colunas_numericas_protegidas = ['Volume', 'Total', 'Valor', 'CPU', 'QTD', 'Rateio', 
                                     'CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516',
                                     'CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%',
                                     'Soma_Percentuais']
    
    # 🔧 CORREÇÃO GENÉRICA: Identificar automaticamente colunas numéricas existentes
    for col in df.columns:
        if col not in colunas_numericas_protegidas:
            try:
                sample = df[col].dropna().head(100)
                if len(sample) > 0:
                    pd.to_numeric(sample, errors='raise')
                    colunas_numericas_protegidas.append(col)
            except (ValueError, TypeError):
                pass
    
    # 🔧 CORREÇÃO CRÍTICA: Garantir que colunas numéricas protegidas sejam sempre numéricas
    for col in colunas_numericas_protegidas:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Para cada coluna do tipo object, converter todos os valores para string
    for col in df.columns:
        if df[col].dtype == 'object' and col not in colunas_numericas_protegidas:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
    
    # 🔧 VALIDAÇÃO FINAL CRÍTICA: Garantir que Volume seja sempre numérico
    if 'Volume' in df.columns:
        try:
            if df['Volume'].dtype in ['int64', 'float64', 'float32', 'int32']:
                volume_antes = df['Volume'].sum()
            else:
                volume_temp = pd.to_numeric(df['Volume'], errors='coerce')
                volume_antes = volume_temp.fillna(0).sum()
        except:
            volume_antes = 0
        
        if df['Volume'].dtype in ['int64', 'float64', 'float32', 'int32']:
            df['Volume'] = df['Volume'].astype('float64')
        elif df['Volume'].dtype == 'object':
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
            df['Volume'] = df['Volume'].astype('float64')
            if df['Volume'].isna().any():
                df['Volume'] = df['Volume'].fillna(0)
        else:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')
        
        volume_depois = df['Volume'].sum()
        if abs(volume_antes - volume_depois) > 0.01 and volume_antes > 0:
            print(f"⚠️ ATENÇÃO: Volume mudou durante normalização! Antes: {volume_antes:,.0f}, Depois: {volume_depois:,.0f}")
    
    if 'Volume' in df.columns:
        if df['Volume'].dtype == 'object' or (hasattr(df['Volume'].dtype, 'name') and df['Volume'].dtype.name == 'string'):
            print(f"⚠️ ERRO CRÍTICO: Volume ainda é {df['Volume'].dtype} após normalização!")
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')
        if df['Volume'].dtype != 'float64':
            df['Volume'] = df['Volume'].astype('float64')
    
    return df


def configurar_ano(ano: Optional[int] = None, continuar_sem_arquivos: bool = False) -> Dict[str, any]:
    """
    Configura o ano e estrutura de pastas (Célula 0 do notebook)
    
    Args:
        ano: Ano para processar. Se None, usa o ano atual
        continuar_sem_arquivos: Se True, continua mesmo sem arquivos necessários
    
    Returns:
        Dicionário com configurações (PASTA_ANO, CAMINHOS, etc.)
    """
    if ano is None:
        ano = datetime.now().year
    
    pasta_ano = os.path.join(_DATA_ROOT, str(ano))
    pasta_historico = os.path.join(_DATA_ROOT, 'historico_consolidado')
    pasta_raiz = '.'
    
    # Criar estrutura de pastas
    os.makedirs(pasta_ano, exist_ok=True)
    os.makedirs(pasta_historico, exist_ok=True)
    
    # Verificar arquivos
    arquivos_necessarios = {
        'Dados SAPIENS.xlsx': 'Base de dados SAPIENS',
        'Reporting fluxo anexo.xlsx': 'Dados de rateio/volume e Sapiens'
    }
    
    arquivos_ok = []
    arquivos_faltando = []
    
    for arquivo, descricao in arquivos_necessarios.items():
        caminho_ano = os.path.join(pasta_ano, arquivo)
        caminho_raiz = os.path.join(pasta_raiz, arquivo)
        
        if os.path.exists(caminho_ano):
            arquivos_ok.append(arquivo)
        elif os.path.exists(caminho_raiz):
            shutil.copy2(caminho_raiz, caminho_ano)
            arquivos_ok.append(arquivo)
        else:
            arquivos_faltando.append((arquivo, descricao))
    
    if arquivos_faltando and not continuar_sem_arquivos:
        raise Exception(f"❌ Arquivos não encontrados: {[a[0] for a in arquivos_faltando]}")
    
    # Definir caminhos
    caminho_sapiens = os.path.join(pasta_ano, 'Dados SAPIENS.xlsx')
    caminho_rateio = os.path.join(pasta_ano, 'Reporting fluxo anexo.xlsx')
    
    # Caminhos de saída (parquets na pasta do ano)
    caminho_df_final = os.path.join(pasta_ano, 'df_final.parquet')
    caminho_df_vol = os.path.join(pasta_ano, 'df_vol.parquet')
    caminho_df_ke5z_group = os.path.join(pasta_ano, 'df_ke5z_group.parquet')
    
    # Caminhos de saída (Excel na pasta do ano)
    caminho_df_final_xlsx = os.path.join(pasta_ano, 'df_final.xlsx')
    caminho_df_vol_xlsx = os.path.join(pasta_ano, 'df_vol.xlsx')
    caminho_df_ke5z_group_xlsx = os.path.join(pasta_ano, 'df_ke5z_group.xlsx')
    caminho_df_final_cpu_xlsx = os.path.join(pasta_ano, 'df_final_cpu.xlsx')
    
    # Caminhos do histórico consolidado
    caminho_historico_final = os.path.join(pasta_historico, 'df_final_historico.parquet')
    caminho_historico_vol = os.path.join(pasta_historico, 'df_vol_historico.parquet')
    caminho_historico_ke5z = os.path.join(pasta_historico, 'df_ke5z_historico.parquet')
    
    return {
        'ANO_ATUAL': ano,
        'PASTA_ANO': pasta_ano,
        'PASTA_HISTORICO': pasta_historico,
        'CAMINHO_SAPIENS': caminho_sapiens,
        'CAMINHO_RATEIO': caminho_rateio,
        'CAMINHO_DF_FINAL': caminho_df_final,
        'CAMINHO_DF_VOL': caminho_df_vol,
        'CAMINHO_DF_KE5Z_GROUP': caminho_df_ke5z_group,
        'CAMINHO_DF_FINAL_XLSX': caminho_df_final_xlsx,
        'CAMINHO_DF_VOL_XLSX': caminho_df_vol_xlsx,
        'CAMINHO_DF_KE5Z_GROUP_XLSX': caminho_df_ke5z_group_xlsx,
        'CAMINHO_DF_FINAL_CPU_XLSX': caminho_df_final_cpu_xlsx,
        'CAMINHO_HISTORICO_FINAL': caminho_historico_final,
        'CAMINHO_HISTORICO_VOL': caminho_historico_vol,
        'CAMINHO_HISTORICO_KE5Z': caminho_historico_ke5z,
        'arquivos_ok': arquivos_ok,
        'arquivos_faltando': arquivos_faltando
    }


def processar_dados_reais(config: Dict[str, any], progress_callback=None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Processa dados REAIS seguindo a lógica completa do notebook dados.ipynb
    
    Args:
        config: Dicionário de configuração retornado por configurar_ano()
        progress_callback: Função opcional para reportar progresso (recebe mensagem: str)
    
    Returns:
        Tupla (df_final, df_vol, df_ke5z_group)
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)
    
    log("📊 Pré-validação dos arquivos...")
    _validar_abas_excel(config['CAMINHO_RATEIO'], ['Sapiens', 'Rateio', 'Volume'], "Reporting fluxo anexo.xlsx")
    _validar_abas_excel(config['CAMINHO_SAPIENS'], ['Base conso'], "Dados SAPIENS.xlsx")

    log("📊 Lendo dados KE5Z...")
    # Célula 1: Ler dados Sapiens
    df_KE5Z = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name='Sapiens', header=1)
    df_KE5Z = limpar_colunas_duplicadas(df_KE5Z)  # 🔧 Limpar colunas duplicadas
    df_KE5Z = _aplicar_alias_colunas(
        df_KE5Z,
        {
            'Nºconta': ['Nºconta', 'N°conta', 'Nº conta', 'N° conta', 'No conta', 'Noconta'],
            'Período': ['Período', 'Periodo'],
            'Veículo': ['Veículo', 'Veiculo', 'Veculo'],
        },
    )
    _exigir_colunas(df_KE5Z, ['Valor', 'QTD', 'Nºconta', 'Oficina', 'Período', 'Account', 'USI'], "aba 'Sapiens'")
    df_KE5Z['Valor'] = pd.to_numeric(df_KE5Z['Valor'], errors='coerce').fillna(0)
    df_KE5Z['QTD'] = pd.to_numeric(df_KE5Z['QTD'], errors='coerce').fillna(0)
    df_KE5Z = df_KE5Z[df_KE5Z['Nºconta'].notna() & (df_KE5Z['Valor'] != 0)]

    # 🔧 Garantir consistência de Período (janeiro vs Janeiro) antes de qualquer merge
    df_KE5Z = normalizar_coluna_periodo(df_KE5Z, 'Período')
    
    log("🔗 Fazendo merge com Base Conso...")
    # Célula 2: Merge com Base Conso
    df_base_conso = pd.read_excel(config['CAMINHO_SAPIENS'], sheet_name='Base conso')
    df_base_conso = limpar_colunas_duplicadas(df_base_conso)  # 🔧 Limpar colunas duplicadas
    df_base_conso = _aplicar_alias_colunas(
        df_base_conso,
        {
            'Type 04': ['Type 04', 'Type04', 'Type_04'],
            'Type 07': ['Type 07', 'Type07', 'Type_07'],
        },
    )
    if 'Type 04' in df_base_conso.columns:
        df_base_conso = df_base_conso.rename(columns={'Type 04': 'Custo'})
    _exigir_colunas(df_base_conso, ['Custo', 'Type 07'], "aba 'Base conso'")
    df_base_conso = df_base_conso[['Custo', 'Type 07']].rename(columns={'Type 07': 'Account'})
    df_KE5Z = pd.merge(
        df_KE5Z,
        df_base_conso[['Custo', 'Account']],
        on='Account',
        how='left',
        suffixes=('', '_conso')
    )

    # 🔧 Alguns anos trazem uma coluna "Custo" na aba Sapiens; preferir a Base Conso quando existir
    if 'Custo_conso' in df_KE5Z.columns:
        if 'Custo' in df_KE5Z.columns:
            df_KE5Z['Custo'] = df_KE5Z['Custo_conso'].combine_first(df_KE5Z['Custo'])
            df_KE5Z = df_KE5Z.drop(columns=['Custo_conso'])
        else:
            df_KE5Z = df_KE5Z.rename(columns={'Custo_conso': 'Custo'})
    
    log("📊 Processando rateio...")
    # Célula 3: Processar Rateio
    df_raw = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name='Rateio', header=None)
    df_raw = limpar_colunas_duplicadas(df_raw)  # 🔧 Limpar colunas duplicadas
    df = df_raw.iloc[1:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.loc[:, df.notna().any(axis=0)]
    df = df.dropna(axis=1, how='all')

    df = _aplicar_alias_colunas(df, {'Veículo': ['Veículo', 'Veiculo']})
    _exigir_colunas(df, ['Oficina', 'Veículo'], "aba 'Rateio'")
    
    meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
             'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    colunas_meses = _encontrar_colunas_meses(df)
    if not colunas_meses:
        raise ValueError(
            "❌ Não encontrei colunas de meses na aba 'Rateio'. "
            "Verifique se existem colunas como Janeiro, Fevereiro, ... e se o header está na linha esperada."
        )
    colunas_id = [col for col in df.columns if col not in colunas_meses and pd.notna(col)]
    df = df.loc[:, df.columns.notna()]

    # 🔧 Robustez: manter somente o que é usado downstream
    colunas_id_usadas = [c for c in ['Oficina', 'Veículo'] if c in df.columns]
    df = df.melt(id_vars=colunas_id_usadas, value_vars=colunas_meses, var_name='Mês', value_name='Rateio')
    df['Rateio'] = pd.to_numeric(df['Rateio'], errors='coerce').fillna(0)
    df = df.rename(columns={'Mês': 'Período'})
    # Meses da aba Rateio são apenas nomes (sem ano) -> normalizar por prefixo, robusto a encoding
    df['Período'] = df['Período'].apply(_normalizar_nome_mes)
    if 'Veículo' in df.columns:
        df['Veículo'] = df['Veículo'].astype(str).str.strip()
    df = df[df['Oficina'] != 'Veículos']
    df = df[df['Oficina'].notna()]
    
    log("🔄 Fazendo merge KE5Z ↔ Rateio...")
    # Célula 4: Merge KE5Z ↔ Rateio e cálculo por veículo
    df_merge = pd.merge(df_KE5Z, df, on=['Oficina', 'Período'], how='left', suffixes=('', '_df'))

    if 'Veículo' not in df_merge.columns:
        raise KeyError("Coluna 'Veículo' não encontrada após leitura da aba Rateio.")

    df_pivot = df_merge.pivot_table(
        index=['Oficina', 'Período'],
        columns='Veículo',
        values='Rateio',
        aggfunc='mean'
    ).reset_index()
    df_pivot.columns.name = None

    veiculos_pivot = [col for col in df_pivot.columns if col not in ['Oficina', 'Período']]

    # 🔧 Evitar colunas _x/_y quando a aba Sapiens já traz colunas com nomes iguais aos veículos
    df_KE5Z_sem_conflito = df_KE5Z.drop(columns=[c for c in veiculos_pivot if c in df_KE5Z.columns], errors='ignore')
    df_final = pd.merge(df_KE5Z_sem_conflito, df_pivot, on=['Oficina', 'Período'], how='left')

    # Renomear colunas de rateio para sufixo % e converter para numérico
    rename_dict = {col: f"{col}%" for col in veiculos_pivot if col in df_final.columns}
    df_final = df_final.rename(columns=rename_dict)
    veiculos_cols_pct = [f"{col}%" for col in veiculos_pivot if f"{col}%" in df_final.columns]

    for col_pct in veiculos_cols_pct:
        if df_final[col_pct].dtype == 'object':
            df_final[col_pct] = df_final[col_pct].astype(str).str.replace('%', '', regex=False).str.strip()
        df_final[col_pct] = pd.to_numeric(df_final[col_pct], errors='coerce').astype(np.float64).fillna(0.0)

        # Se vier como 32.3 (percentual em 0-100), converter para 0-1
        try:
            amostra_max = float(df_final[col_pct].dropna().head(1000).max()) if len(df_final) else 0.0
        except Exception:
            amostra_max = 0.0
        if amostra_max > 1.5:
            df_final[col_pct] = df_final[col_pct] / 100.0
    
    log("💾 Calculando valores por veículo...")
    # Célula 5: Criar colunas calculadas
    df_final['Valor'] = pd.to_numeric(df_final['Valor'], errors='coerce').fillna(0)
    for col_pct in veiculos_cols_pct:
        col_nome = col_pct[:-1]  # remove '%'
        df_final[col_nome] = df_final[col_pct] * df_final['Valor']
    
    # Célula 6: Análise de soma dos percentuais (opcional, para diagnóstico)
    if len(veiculos_cols_pct) > 0:
        df_final['Soma_Percentuais'] = df_final[veiculos_cols_pct].sum(axis=1)
        linhas_com_rateio = (df_final['Soma_Percentuais'] > 0).sum()
        log(f"📊 Análise: {linhas_com_rateio:,} linhas com rateios")
    
    # Célula 7: Somatória de cada coluna (opcional, para diagnóstico)
    colunas_para_somar = [col[:-1] for col in veiculos_cols_pct]
    soma_total = 0
    for col in colunas_para_somar:
        if col in df_final.columns:
            soma = pd.to_numeric(df_final[col], errors='coerce').fillna(0).sum()
            soma_total += soma
    
    # Célula 5: Salvar Excel intermediário (df_final.xlsx)
    if 'CAMINHO_DF_FINAL_XLSX' in config:
        df_final.to_excel(config['CAMINHO_DF_FINAL_XLSX'], index=False)
        log(f"💾 Excel intermediário salvo: {config['CAMINHO_DF_FINAL_XLSX']}")
    
    # Célula 8: Remover colunas de percentual
    if len(veiculos_cols_pct) > 0:
        df_final = df_final.drop(columns=[col for col in veiculos_cols_pct if col in df_final.columns])
    
    # Célula 9: Transformar veículos em linhas
    colunas_veiculos = [col[:-1] for col in veiculos_cols_pct]
    colunas_veiculos_existentes = [col for col in colunas_veiculos if col in df_final.columns]
    
    if len(colunas_veiculos_existentes) > 0:
        # Alguns relatórios (ex.: 2026) já trazem uma coluna chamada "Total".
        # O melt cria uma coluna com value_name='Total', então precisamos evitar conflito.
        if 'Total' in df_final.columns:
            df_final = df_final.rename(columns={'Total': 'Total_Original'})
        colunas_id = [col for col in df_final.columns if col not in colunas_veiculos]
        df_final = df_final.melt(id_vars=colunas_id, value_vars=colunas_veiculos_existentes, var_name='Veículo', value_name='Total')
    
    log("📈 Processando volume...")
    # Célula 10: Processar Volume
    df_ke5z_volume = _ler_aba_volume_auto(config['CAMINHO_RATEIO'], sheet_name='Volume')
    df_ke5z_volume = limpar_colunas_duplicadas(df_ke5z_volume)  # 🔧 Limpar colunas duplicadas

    # 🔧 Robustez: normalizar nome da coluna Veículo (há anos que vêm como "Veiculo")
    df_ke5z_volume = _aplicar_alias_colunas(
        df_ke5z_volume,
        {
            'Veículo': ['Veículo', 'Veiculo', 'Veculo'],
        },
    )

    # Regra esperada: aba Volume (REAIS) deve conter a dimensão Veículo
    if 'Veículo' not in df_ke5z_volume.columns:
        cols = ", ".join([str(c) for c in df_ke5z_volume.columns])
        raise ValueError(
            "❌ Aba 'Volume' (REAIS) sem a coluna 'Veículo'. "
            "Isso indica erro de layout/header ou nome de coluna diferente. "
            f"Colunas encontradas: {cols}"
        )
    
    # 🔧 CORREÇÃO CRÍTICA: Remover TODAS as colunas Unnamed: (não apenas Unnamed: 14)
    # Isso previne colunas vazias do Excel que causam duplicação ao consolidar
    colunas_unnamed = [col for col in df_ke5z_volume.columns if 'Unnamed:' in str(col)]
    if colunas_unnamed:
        log(f"⚠️ Removendo {len(colunas_unnamed)} colunas 'Unnamed:' vazias do Excel")
        df_ke5z_volume = df_ke5z_volume.drop(columns=colunas_unnamed)
    
    mapeamento_meses = {
        'janeiro': 'Janeiro', 'fevereiro': 'Fevereiro', 'março': 'Março',
        'abril': 'Abril', 'maio': 'Maio', 'junho': 'Junho',
        'julho': 'Julho', 'agosto': 'Agosto', 'setembro': 'Setembro',
        'outubro': 'Outubro', 'novembro': 'Novembro', 'dezembro': 'Dezembro'
    }
    
    colunas_meses_encontradas = _encontrar_colunas_meses(df_ke5z_volume)

    if not colunas_meses_encontradas:
        raise ValueError(
            "❌ Não encontrei colunas de meses na aba 'Volume' (header=50). "
            "Verifique se as colunas Janeiro..Dezembro existem e se o header está correto."
        )
    
    # 🔧 Robustez: manter apenas dimensões necessárias (evita colunas extras causarem duplicação)
    colunas_id_vol = ['Oficina', 'Veículo']
    df_vol = pd.melt(
        df_ke5z_volume,
        id_vars=colunas_id_vol,
        value_vars=colunas_meses_encontradas,
        var_name='Período',
        value_name='Volume'
    )
    
    df_vol['Período'] = df_vol['Período'].apply(_normalizar_nome_mes)
    
    _exigir_colunas(df_vol, ['Oficina', 'Veículo', 'Período', 'Volume'], "aba 'Volume' após melt")
    df_vol['Volume'] = pd.to_numeric(df_vol['Volume'], errors='coerce').fillna(0)
    df_vol = df_vol[df_vol['Oficina'].notna() & df_vol['Período'].notna()]

    # Consolidar no grão correto (Oficina/Veículo/Período)
    df_vol = df_vol[df_vol['Veículo'].notna()].copy()
    df_vol['Veículo'] = df_vol['Veículo'].astype(str).str.strip()
    colunas_chave_vol = ['Oficina', 'Veículo', 'Período']
    df_vol = df_vol[colunas_chave_vol + ['Volume']].drop_duplicates()
    df_vol = df_vol.groupby(colunas_chave_vol, as_index=False, dropna=False)['Volume'].sum()
    df_vol['Volume'] = df_vol['Volume'].astype('float64')
    
    log("🔍 Aplicando filtros...")
    # Célula 11: Diagnóstico antes do filtro de Account
    df_antes_filtro = df_final.copy()
    total_antes = df_antes_filtro['Total'].sum() if 'Total' in df_antes_filtro.columns else 0
    
    # Célula 11: Filtrar Account
    df_final = df_final[df_final['Account'].notna() & (df_final['Account'] != 0) & (df_final['Account'] != 'TC Ext')]
    
    # Célula 11: Salvar Excel após filtro Account (df_final_cpu.xlsx)
    if 'CAMINHO_DF_FINAL_CPU_XLSX' in config:
        df_final.to_excel(config['CAMINHO_DF_FINAL_CPU_XLSX'], index=False)
        log(f"💾 Excel após filtro Account salvo: {config['CAMINHO_DF_FINAL_CPU_XLSX']}")
    
    df_final = df_final[df_final['USI'] == 'TC Ext']
    
    total_apos = df_final['Total'].sum() if 'Total' in df_final.columns else 0
    log(f"📊 Total antes filtros: {total_antes:,.2f} | Após filtros: {total_apos:,.2f}")
    
    log("📊 Agrupando dados...")
    # Célula 12: Agrupar Volume e fazer merge final
    df_KE5Z = df_KE5Z[df_KE5Z['USI'] == 'TC Ext']
    df_KE5Z = df_KE5Z[df_KE5Z['Account'].notna() & (df_KE5Z['Account'] != 0) & (df_KE5Z['Account'] != '')]
    
    if 'Volume' not in df_KE5Z.columns:
        df_KE5Z['Volume'] = 0
    df_KE5Z['Volume'] = pd.to_numeric(df_KE5Z['Volume'], errors='coerce').fillna(0)
    
    if 'Total' not in df_KE5Z.columns:
        if 'Valor' in df_KE5Z.columns:
            df_KE5Z['Total'] = df_KE5Z['Valor']
        else:
            df_KE5Z['Total'] = 0
    df_KE5Z['Total'] = pd.to_numeric(df_KE5Z['Total'], errors='coerce').fillna(0)
    
    if 'Período' in df_KE5Z.columns:
        df_KE5Z['Período'] = df_KE5Z['Período'].astype(str).str.strip()
        for mes_min, mes_cap in mapeamento_meses.items():
            df_KE5Z['Período'] = df_KE5Z['Período'].str.replace(mes_min, mes_cap, case=False, regex=False)
    
    # Célula 12: Agrupar Volume e fazer merge final (usar Veículo quando houver)
    colunas_merge_ke5z_vol = ['Oficina', 'Período']
    if 'Veículo' in df_KE5Z.columns and 'Veículo' in df_vol.columns and df_vol['Veículo'].notna().any():
        colunas_merge_ke5z_vol.append('Veículo')
    df_vol_group = df_vol.groupby(colunas_merge_ke5z_vol, as_index=False, dropna=False)['Volume'].sum()
    
    df_ke5z_group = pd.merge(
        df_KE5Z.drop(columns=[col for col in df_KE5Z.columns if col.lower() == 'volume']),
        df_vol_group,
        on=colunas_merge_ke5z_vol,
        how='left'
    )
    
    # Célula 12: Salvar Excel do df_ke5z_group
    if 'CAMINHO_DF_KE5Z_GROUP_XLSX' in config:
        df_ke5z_group.to_excel(config['CAMINHO_DF_KE5Z_GROUP_XLSX'], index=False)
        log(f"💾 Excel df_ke5z_group salvo: {config['CAMINHO_DF_KE5Z_GROUP_XLSX']}")
    
    log("✅ Processamento concluído!")
    return df_final, df_vol, df_ke5z_group


def salvar_e_consolidar(df_final: pd.DataFrame, df_vol: pd.DataFrame, df_ke5z_group: pd.DataFrame, 
                        config: Dict[str, any], progress_callback=None):
    """
    Salva os DataFrames e consolida com histórico (Célula 13 do notebook)
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)
    
    log("💾 Salvando arquivos...")
    
    # Adicionar coluna Ano
    if 'Ano' not in df_final.columns:
        df_final['Ano'] = config['ANO_ATUAL']
    if 'Ano' not in df_vol.columns:
        df_vol['Ano'] = config['ANO_ATUAL']
    if 'Ano' not in df_ke5z_group.columns:
        df_ke5z_group['Ano'] = config['ANO_ATUAL']
    
    # Garantir Volume seja float64 antes de normalizar
    if 'Volume' in df_vol.columns:
        df_vol['Volume'] = df_vol['Volume'].astype('float64')
    
    # Normalizar e salvar
    df_final = normalizar_tipos_para_parquet(df_final)
    df_final.to_parquet(config['CAMINHO_DF_FINAL'])
    
    df_vol = normalizar_tipos_para_parquet(df_vol)
    if 'Volume' in df_vol.columns:
        df_vol['Volume'] = df_vol['Volume'].astype('float64')
    df_vol.to_parquet(config['CAMINHO_DF_VOL'])
    
    df_ke5z_group = normalizar_tipos_para_parquet(df_ke5z_group)
    df_ke5z_group.to_parquet(config['CAMINHO_DF_KE5Z_GROUP'])
    
    log("📊 Salvando arquivos Excel...")
    # Célula 13: Salvar todos os arquivos Excel
    if 'CAMINHO_DF_FINAL_XLSX' in config:
        df_final.to_excel(config['CAMINHO_DF_FINAL_XLSX'], index=False)
        log(f"   ✅ df_final.xlsx salvo")
    
    if 'CAMINHO_DF_VOL_XLSX' in config:
        df_vol.to_excel(config['CAMINHO_DF_VOL_XLSX'], index=False)
        log(f"   ✅ df_vol.xlsx salvo")
    
    if 'CAMINHO_DF_KE5Z_GROUP_XLSX' in config:
        df_ke5z_group.to_excel(config['CAMINHO_DF_KE5Z_GROUP_XLSX'], index=False)
        log(f"   ✅ df_ke5z_group.xlsx salvo")
    
    log("📚 Consolidando histórico...")
    
    # Consolidar histórico
    pasta_dados = _DATA_ROOT
    anos_disponiveis = []
    if os.path.exists(pasta_dados):
        for item in os.listdir(pasta_dados):
            caminho_item = os.path.join(pasta_dados, item)
            if os.path.isdir(caminho_item) and item.isdigit():
                anos_disponiveis.append(int(item))
    
    anos_disponiveis = sorted(anos_disponiveis)
    
    def consolidar_historico(df_novo, caminho_historico, nome_df):
        dfs_todos_anos = []
        for ano in anos_disponiveis:
            if nome_df == 'df_final':
                caminho_ano = os.path.join(pasta_dados, str(ano), 'df_final.parquet')
            elif nome_df == 'df_vol':
                caminho_ano = os.path.join(pasta_dados, str(ano), 'df_vol.parquet')
            elif nome_df == 'df_ke5z_group':
                caminho_ano = os.path.join(pasta_dados, str(ano), 'df_ke5z_group.parquet')
            else:
                continue
            
            if os.path.exists(caminho_ano):
                try:
                    df_ano = pd.read_parquet(caminho_ano)
                    # 🔧 CORREÇÃO CRÍTICA: Remover colunas duplicadas (com sufixos .1, .2, etc)
                    # Isso previne a propagação de colunas duplicadas ao consolidar múltiplos anos
                    colunas_originais = []
                    colunas_para_remover = []
                    for col in df_ano.columns:
                        # Remover sufixos .1, .2, etc de nomes de colunas duplicadas
                        col_base = col.split('.')[0] if '.' in str(col) and str(col).split('.')[-1].isdigit() else col
                        if col_base not in colunas_originais:
                            colunas_originais.append(col_base)
                        else:
                            # Coluna duplicada, marcar para remoção
                            colunas_para_remover.append(col)
                    
                    if colunas_para_remover:
                        log(f"⚠️ Removendo {len(colunas_para_remover)} colunas duplicadas do ano {ano}: {colunas_para_remover[:5]}")
                        df_ano = df_ano.drop(columns=colunas_para_remover)
                    
                    if 'Volume' in df_ano.columns:
                        df_ano['Volume'] = pd.to_numeric(df_ano['Volume'], errors='coerce').fillna(0).astype('float64')
                    if 'Ano' not in df_ano.columns:
                        df_ano['Ano'] = ano
                    dfs_todos_anos.append(df_ano)
                except Exception as e:
                    log(f"⚠️ Erro ao carregar {caminho_ano}: {e}")
        
        if len(dfs_todos_anos) > 0:
            df_consolidado = pd.concat(dfs_todos_anos, ignore_index=True)
            if 'Volume' in df_consolidado.columns:
                df_consolidado['Volume'] = pd.to_numeric(df_consolidado['Volume'], errors='coerce').fillna(0).astype('float64')
            df_consolidado = normalizar_tipos_para_parquet(df_consolidado)
            if 'Volume' in df_consolidado.columns:
                df_consolidado['Volume'] = df_consolidado['Volume'].astype('float64')
            df_consolidado.to_parquet(caminho_historico)
            return df_consolidado
        else:
            df_consolidado = df_novo
            df_consolidado = normalizar_tipos_para_parquet(df_consolidado)
            df_consolidado.to_parquet(caminho_historico)
            return df_consolidado
    
    consolidar_historico(df_final, config['CAMINHO_HISTORICO_FINAL'], 'df_final')
    consolidar_historico(df_vol, config['CAMINHO_HISTORICO_VOL'], 'df_vol')
    consolidar_historico(df_ke5z_group, config['CAMINHO_HISTORICO_KE5Z'], 'df_ke5z_group')
    
    log("✅ Salvamento e consolidação concluídos!")


def processar_completo(ano: Optional[int] = None, continuar_sem_arquivos: bool = False, 
                      progress_callback=None) -> Dict[str, any]:
    """
    Executa o processamento completo de dados REAIS
    
    Args:
        ano: Ano para processar. Se None, usa o ano atual
        continuar_sem_arquivos: Se True, continua mesmo sem arquivos necessários
        progress_callback: Função opcional para reportar progresso
    
    Returns:
        Dicionário com informações do processamento
    """
    config = configurar_ano(ano, continuar_sem_arquivos)
    df_final, df_vol, df_ke5z_group = processar_dados_reais(config, progress_callback)
    salvar_e_consolidar(df_final, df_vol, df_ke5z_group, config, progress_callback)
    
    return {
        'sucesso': True,
        'ano': config['ANO_ATUAL'],
        'df_final_linhas': len(df_final),
        'df_vol_linhas': len(df_vol),
        'df_ke5z_group_linhas': len(df_ke5z_group)
    }

