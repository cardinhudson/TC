"""
Módulo de Processamento de Dados BUDGET
Convertido do notebook dados_BUD.ipynb mantendo toda a lógica original
"""

import pandas as pd
import numpy as np
import os
import shutil
from datetime import datetime
from typing import Tuple, Dict, Optional
import re
import unicodedata

# ═══════════════════════════════════════════════════════════════
#  OFICINAS INVÁLIDAS (excluídas da extração)
# ═══════════════════════════════════════════════════════════════
# Lista de oficinas que existem nos dados fonte mas não devem ser processadas
OFICINAS_INVALIDAS = ['Veículos', 'Projetos']


def limpar_periodo_sufixos(df):
    """Remove sufixos .1, .2, .3 da coluna Período"""
    if 'Período' not in df.columns:
        return df
    
    df = df.copy()
    
    # Remover sufixos .1, .2, .3, etc da coluna Período
    df['Período'] = df['Período'].astype(str).str.replace(r'\.\d+$', '', regex=True)
    
    return df


def filtrar_oficinas_validas(df: pd.DataFrame, contexto: str = "") -> pd.DataFrame:
    """Remove oficinas inválidas do DataFrame.
    
    Args:
        df: DataFrame com coluna 'Oficina'
        contexto: Contexto para mensagem de log (ex: "do Rateio BDG ")
    
    Returns:
        DataFrame filtrado sem oficinas inválidas
    """
    if df is None or df.empty or 'Oficina' not in df.columns:
        return df
    
    mask_invalida = df['Oficina'].isin(OFICINAS_INVALIDAS)
    n_excluidas = mask_invalida.sum()
    
    if n_excluidas > 0:
        oficinas_removidas = sorted(df.loc[mask_invalida, 'Oficina'].unique().tolist())
        df = df[~mask_invalida].copy()
        print(f"   ℹ️ {n_excluidas:,} linhas excluídas {contexto}(oficinas inválidas: {oficinas_removidas})")
    
    return df


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


def limpar_colunas_duplicadas(df):
    """Remove colunas duplicadas e inválidas do DataFrame (mesma lógica do app.py)"""
    if df is None or df.empty:
        return df
    
    colunas_originais = list(df.columns)
    colunas_duplicadas = []
    
    # 1. Remover colunas Unnamed
    colunas_unnamed = [col for col in df.columns if isinstance(col, str) and 'Unnamed:' in col]
    if colunas_unnamed:
        df = df.drop(columns=colunas_unnamed)
        colunas_duplicadas.extend(colunas_unnamed)
    
    # 2. Remover colunas com sufixo .1, .2, etc (EXCETO coluna Período!)
    colunas_sufixo = []
    for col in df.columns:
        if col == 'Período':
            continue
        if isinstance(col, str) and re.search(r'\.\d+$', col):
            colunas_sufixo.append(col)
    
    if colunas_sufixo:
        df = df.drop(columns=colunas_sufixo)
        colunas_duplicadas.extend(colunas_sufixo)
    
    # 3. Remover colunas completamente vazias
    colunas_vazias = [col for col in df.columns if df[col].isna().all()]
    if colunas_vazias:
        df = df.drop(columns=colunas_vazias)
        colunas_duplicadas.extend(colunas_vazias)
    
    # 4. Remover duplicatas baseadas em nome e conteúdo
    colunas_vistas = {}
    colunas_remover = []
    
    for col in df.columns:
        if col == 'Período':
            continue
        col_base = re.sub(r'\.\d+$', '', str(col))
        if col_base in colunas_vistas:
            col_original = colunas_vistas[col_base]
            if df[col].equals(df[col_original]):
                colunas_remover.append(col)
        else:
            colunas_vistas[col_base] = col
    
    if colunas_remover:
        df = df.drop(columns=colunas_remover)
        colunas_duplicadas.extend(colunas_remover)
    
    if colunas_duplicadas:
        try:
            print(f"WARNING: Limpeza automática: {len(colunas_duplicadas)} colunas duplicadas/inválidas removidas")
            print(f"   Colunas removidas: {colunas_duplicadas}")
        except Exception:
            # Evitar falhas de encoding no console
            pass
    
    return df


def normalizar_tipos_para_parquet(df):
    """Normaliza tipos de dados para evitar erros ao salvar parquet.
    Mesma função do processamento_dados.py
    """
    df = df.copy()
    
    colunas_numericas_protegidas = ['Volume', 'Total', 'Valor', 'CPU', 'QTD', 'Rateio', 
                                     'CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516',
                                     'CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%',
                                     'Soma_Percentuais']
    
    for col in df.columns:
        if col not in colunas_numericas_protegidas:
            try:
                sample = df[col].dropna().head(100)
                if len(sample) > 0:
                    pd.to_numeric(sample, errors='raise')
                    colunas_numericas_protegidas.append(col)
            except (ValueError, TypeError):
                pass
    
    for col in colunas_numericas_protegidas:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    for col in df.columns:
        if df[col].dtype == 'object' and col not in colunas_numericas_protegidas:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
    
    if 'Volume' in df.columns:
        if df['Volume'].dtype in ['int64', 'float64', 'float32', 'int32']:
            df['Volume'] = df['Volume'].astype('float64')
        elif df['Volume'].dtype == 'object':
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
            df['Volume'] = df['Volume'].astype('float64')
            if df['Volume'].isna().any():
                df['Volume'] = df['Volume'].fillna(0)
        else:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')
    
    if 'Volume' in df.columns:
        if df['Volume'].dtype == 'object' or (hasattr(df['Volume'].dtype, 'name') and df['Volume'].dtype.name == 'string'):
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype('float64')
        if df['Volume'].dtype != 'float64':
            df['Volume'] = df['Volume'].astype('float64')
    
    return df


def configurar_ano_bud(ano: Optional[int] = None, continuar_sem_arquivos: bool = False) -> Dict[str, any]:
    """
    Configura o ano e estrutura de pastas para BUDGET (Célula 0 do notebook BUD)
    """
    if ano is None:
        ano = datetime.now().year
    
    pasta_ano = f'dados/TC_Ext/{ano}'
    pasta_bud = f'dados/TC_Ext/{ano}/BUD'
    pasta_historico = 'dados/TC_Ext/historico_consolidado'
    pasta_historico_bud = 'dados/TC_Ext/historico_consolidado/BUD'
    pasta_raiz = '.'
    
    # Criar estrutura de pastas
    os.makedirs(pasta_ano, exist_ok=True)
    os.makedirs(pasta_bud, exist_ok=True)
    os.makedirs(pasta_historico, exist_ok=True)
    os.makedirs(pasta_historico_bud, exist_ok=True)
    
    # Verificar arquivos
    arquivos_necessarios = {
        'Dados SAPIENS.xlsx': 'Base de dados SAPIENS',
        'Reporting fluxo anexo.xlsx': 'Dados de rateio/volume'
    }
    
    arquivos_ok = []
    arquivos_faltando = []

    def _resolver_e_copiar_para_pasta_ano(nome_arquivo: str) -> str:
        """Garante que o arquivo de entrada fique em dados/{ano}/.

        Ordem de busca (compatibilidade):
        1) dados/{ano}/<arquivo>
        2) raiz do projeto ./<arquivo>  -> copia para dados/{ano}/
        3) dados/{ano}/BUD/<arquivo>   -> copia para dados/{ano}/ (legado; BUD deve ser apenas outputs)
        """

        destino_ano = os.path.join(pasta_ano, nome_arquivo)
        origem_raiz = os.path.join(pasta_raiz, nome_arquivo)
        origem_bud = os.path.join(pasta_bud, nome_arquivo)

        if os.path.exists(destino_ano):
            return destino_ano

        if os.path.exists(origem_raiz):
            try:
                shutil.copy2(origem_raiz, destino_ano)
            except Exception:
                # Se não conseguir copiar, pelo menos usa o arquivo encontrado
                return origem_raiz
            return destino_ano

        if os.path.exists(origem_bud):
            try:
                shutil.copy2(origem_bud, destino_ano)
            except Exception:
                return origem_bud
            return destino_ano

        return destino_ano

    for arquivo, descricao in arquivos_necessarios.items():
        caminho_resolvido = _resolver_e_copiar_para_pasta_ano(arquivo)
        if os.path.exists(caminho_resolvido):
            arquivos_ok.append(arquivo)
        else:
            arquivos_faltando.append((arquivo, descricao))
    
    if arquivos_faltando and not continuar_sem_arquivos:
        raise Exception(f"❌ Arquivos não encontrados: {[a[0] for a in arquivos_faltando]}")
    
    # Definir caminhos
    # Entradas padronizadas em dados/{ano}/
    caminho_sapiens = os.path.join(pasta_ano, 'Dados SAPIENS.xlsx')
    caminho_rateio = os.path.join(pasta_ano, 'Reporting fluxo anexo.xlsx')
    
    caminho_df_final = os.path.join(pasta_bud, 'df_final_BUD.parquet')
    caminho_df_vol = os.path.join(pasta_bud, 'df_vol_BUD.parquet')
    caminho_df_ke5z_group = os.path.join(pasta_bud, 'df_ke5z_group_BUD.parquet')
    
    # Caminhos de saída (Excel na pasta BUD com sufixo BUD)
    caminho_df_final_xlsx = os.path.join(pasta_bud, 'df_final_BUD.xlsx')
    caminho_df_vol_xlsx = os.path.join(pasta_bud, 'df_vol_BUD.xlsx')
    caminho_df_ke5z_group_xlsx = os.path.join(pasta_bud, 'df_ke5z_group_BUD.xlsx')
    caminho_df_final_cpu_xlsx = os.path.join(pasta_bud, 'df_final_cpu_BUD.xlsx')
    
    caminho_historico_final = os.path.join(pasta_historico_bud, 'df_final_historico_BUD.parquet')
    caminho_historico_vol = os.path.join(pasta_historico_bud, 'df_vol_historico_BUD.parquet')
    caminho_historico_ke5z = os.path.join(pasta_historico_bud, 'df_ke5z_historico_BUD.parquet')
    
    return {
        'ANO_ATUAL': ano,
        'PASTA_ANO': pasta_ano,
        'PASTA_BUD': pasta_bud,
        'PASTA_HISTORICO': pasta_historico,
        'PASTA_HISTORICO_BUD': pasta_historico_bud,
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


def processar_dados_budget(config: Dict[str, any], progress_callback=None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Processa dados BUDGET seguindo a lógica completa do notebook dados_BUD.ipynb
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)
    
    log("📊 Pré-validação dos arquivos (BUD)...")
    _validar_abas_excel(config['CAMINHO_RATEIO'], ['Voz de custo BDG', 'Rateio BDG', 'Volume BDG'], "Reporting fluxo anexo.xlsx")
    _validar_abas_excel(config['CAMINHO_SAPIENS'], ['Base conso'], "Dados SAPIENS.xlsx")

    log("📊 Lendo dados KE5Z (BUD)...")
    # Célula 1: Ler guia "Voz de custo BDG"
    df_KE5Z = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name='Voz de custo BDG')
    df_KE5Z = limpar_colunas_duplicadas(df_KE5Z)
    df_KE5Z = limpar_periodo_sufixos(df_KE5Z)
    
    meses_prefixo = {
        'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'
    }

    def _mes_prefixo(col):
        col_low = str(col).strip().lower()
        # remover caracteres problemáticos (ex.: "mar�o") e manter prefixo
        try:
            import unicodedata
            col_low = unicodedata.normalize('NFKD', col_low)
            col_low = ''.join(ch for ch in col_low if not unicodedata.combining(ch))
        except Exception:
            pass
        col_low = ''.join(ch for ch in col_low if ch.isalnum())
        pref = col_low[:3]
        return pref if pref in meses_prefixo else None

    colunas_meses = [col for col in df_KE5Z.columns if _mes_prefixo(col)]
    colunas_id = [col for col in df_KE5Z.columns if col not in colunas_meses]
    
    df_KE5Z = df_KE5Z.melt(
        id_vars=colunas_id,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='Valor'
    )

    df_KE5Z = _aplicar_alias_colunas(df_KE5Z, {'Veículo': ['Veículo', 'Veiculo']})
    _exigir_colunas(df_KE5Z, ['Oficina', 'Período', 'Account', 'Valor'], "aba 'Voz de custo BDG' após melt")
    
    # ═══ EXCLUIR oficinas inválidas ═══
    df_KE5Z = filtrar_oficinas_validas(df_KE5Z, "do Voz de Custo BDG ")
    
    df_KE5Z['Valor'] = pd.to_numeric(df_KE5Z['Valor'], errors='coerce').fillna(0)
    
    mapeamento_meses_prefixo = {
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
    
    df_KE5Z['Período'] = df_KE5Z['Período'].apply(lambda v: mapeamento_meses_prefixo.get(_mes_prefixo(v), str(v).strip().capitalize()))
    
    colunas_para_remover = []
    if 'Var/Fix' in df_KE5Z.columns:
        colunas_para_remover.append('Var/Fix')
    if 'Ano' in df_KE5Z.columns:
        colunas_para_remover.append('Ano')
    if colunas_para_remover:
        df_KE5Z = df_KE5Z.drop(columns=colunas_para_remover)
    
    if 'Custo' in df_KE5Z.columns:
        df_KE5Z = df_KE5Z.drop(columns=['Custo'])
    
    log("🔗 Fazendo merge com Base Conso (BUD)...")
    # Célula 2: Merge com Base Conso
    if os.path.exists(config['CAMINHO_SAPIENS']):
        df_base_conso = pd.read_excel(config['CAMINHO_SAPIENS'], sheet_name='Base conso')
        df_base_conso = limpar_colunas_duplicadas(df_base_conso)
        df_base_conso = limpar_periodo_sufixos(df_base_conso)
        df_base_conso = _aplicar_alias_colunas(
            df_base_conso,
            {
                'Type 04': ['Type 04', 'Type04', 'Type_04'],
                'Type 07': ['Type 07', 'Type07', 'Type_07'],
            },
        )
        if 'Type 04' in df_base_conso.columns:
            df_base_conso = df_base_conso.rename(columns={'Type 04': 'Custo'})
        if 'Custo' in df_base_conso.columns and 'Type 07' in df_base_conso.columns:
            df_base_conso = df_base_conso[['Custo', 'Type 07']].rename(columns={'Type 07': 'Account'})
            df_base_conso = df_base_conso.drop_duplicates(subset=['Account'], keep='first')
            if 'Account' in df_KE5Z.columns:
                df_KE5Z = pd.merge(df_KE5Z, df_base_conso[['Custo', 'Account']], on='Account', how='left')
    
    log("📊 Processando rateio (BUD)...")
    # Célula 3: Processar Rateio BDG
    try:
        df_raw = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name='Rateio BDG', header=None)
        df_raw = limpar_colunas_duplicadas(df_raw)
        df_raw = limpar_periodo_sufixos(df_raw)
    except ValueError as e:
        if "Worksheet named 'Rateio BDG' not found" in str(e):
            xl_file = pd.ExcelFile(config['CAMINHO_RATEIO'])
            guias_similares = [s for s in xl_file.sheet_names if 'rateio' in s.lower() or 'bdg' in s.lower()]
            if guias_similares:
                df_raw = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name=guias_similares[0], header=None)
                df_raw = limpar_colunas_duplicadas(df_raw)
                df_raw = limpar_periodo_sufixos(df_raw)
            else:
                raise
    
    df = df_raw.iloc[1:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.loc[:, df.notna().any(axis=0)]
    df = df.dropna(axis=1, how='all')

    df = _aplicar_alias_colunas(df, {'Veículo': ['Veículo', 'Veiculo']})
    _exigir_colunas(df, ['Oficina', 'Veículo'], "aba 'Rateio BDG'")
    
    colunas_meses = [col for col in df.columns if _mes_prefixo(col)]
    if not colunas_meses:
        raise ValueError(
            "❌ Não encontrei colunas de meses na aba 'Rateio BDG'. "
            "Verifique se existem colunas como Janeiro, Fevereiro, ... e se o header está na linha esperada."
        )
    colunas_id = [col for col in df.columns if col not in colunas_meses and pd.notna(col)]
    df = df.loc[:, df.columns.notna()]

    # 🔧 Robustez: manter somente o que é usado downstream
    colunas_id_usadas = [c for c in ['Oficina', 'Veículo'] if c in df.columns]
    df = df.melt(id_vars=colunas_id_usadas, value_vars=colunas_meses, var_name='Mês', value_name='Rateio')
    df['Rateio'] = pd.to_numeric(df['Rateio'], errors='coerce').fillna(0)
    df = df.rename(columns={'Mês': 'Período'})
    
    df['Período'] = df['Período'].apply(lambda v: mapeamento_meses_prefixo.get(_mes_prefixo(v), str(v).strip().capitalize()))
    
    # ═══ EXCLUIR oficinas inválidas ═══
    df = filtrar_oficinas_validas(df, "do Rateio BDG ")
    df = df[df['Oficina'].notna()]
    
    log("🔄 Fazendo merge KE5Z ↔ Rateio (BUD)...")
    # Célula 4: Merge KE5Z ↔ Rateio e cálculo por veículo
    df_KE5Z['Período'] = df_KE5Z['Período'].astype(str).str.strip()
    df['Período'] = df['Período'].astype(str).str.strip()
    df_KE5Z['Oficina'] = df_KE5Z['Oficina'].astype(str).str.strip()
    df['Oficina'] = df['Oficina'].astype(str).str.strip()
    
    df_merge = pd.merge(df_KE5Z, df, on=['Oficina', 'Período'], how='left', suffixes=('', '_df'))
    df_pivot = df_merge.pivot_table(
        index=['Oficina', 'Período'],
        columns='Veículo',
        values='Rateio',
        aggfunc='mean'
    ).reset_index()
    df_pivot.columns.name = None
    
    df_final = pd.merge(df_KE5Z, df_pivot, on=['Oficina', 'Período'], how='left')
    
    veiculos_cols = [col for col in df_final.columns if col not in df_KE5Z.columns and col not in ['Oficina', 'Período']]
    rename_dict = {col: f"{col}%" for col in veiculos_cols}
    df_final = df_final.rename(columns=rename_dict)
    
    veiculos_cols_pct = [f"{col}%" for col in veiculos_cols]
    veiculos_cols = [col for col in veiculos_cols_pct if col in df_final.columns]
    
    for col in veiculos_cols:
        if df_final[col].dtype == "object":
            df_final[col] = df_final[col].astype(str).str.replace('%', '', regex=False).str.strip()
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce').astype(np.float64).fillna(0.0)
    
    log("💾 Calculando valores por veículo (BUD)...")
    # Célula 5: Criar colunas calculadas
    df_final['Valor'] = pd.to_numeric(df_final['Valor'], errors='coerce').fillna(0)
    veiculos_cols_pct = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']
    
    for col_pct in veiculos_cols_pct:
        if col_pct in df_final.columns:
            col_nome = col_pct.replace('%', '')
            df_final[col_nome] = df_final[col_pct] * df_final['Valor']
    
    # Célula 6: Análise de soma dos percentuais (opcional, para diagnóstico)
    veiculos_cols_pct_analise = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']
    if all(col in df_final.columns for col in veiculos_cols_pct_analise):
        df_final['Soma_Percentuais'] = df_final[veiculos_cols_pct_analise].sum(axis=1)
        linhas_com_rateio = (df_final['Soma_Percentuais'] > 0).sum()
        log(f"📊 Análise BUD: {linhas_com_rateio:,} linhas com rateios")
    
    # Célula 7: Somatória de cada coluna (opcional, para diagnóstico)
    colunas_para_somar = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
    soma_total = 0
    for col in colunas_para_somar:
        if col in df_final.columns:
            soma = pd.to_numeric(df_final[col], errors='coerce').fillna(0).sum()
            soma_total += soma
    
    # Célula 5: Salvar Excel intermediário (df_final_BUD.xlsx)
    if 'CAMINHO_DF_FINAL_XLSX' in config:
        df_final.to_excel(config['CAMINHO_DF_FINAL_XLSX'], index=False)
        log(f"💾 Excel intermediário BUD salvo: {config['CAMINHO_DF_FINAL_XLSX']}")
    
    # Célula 8: Remover colunas de percentual
    colunas_para_remover = ['CC21%', 'CC22%', 'CC24%', 'CC24 5L%', 'CC24 7L%', 'J516%']
    for col in colunas_para_remover:
        if col in df_final.columns:
            df_final = df_final.drop(columns=[col])
    
    # Célula 9: Transformar veículos em linhas
    colunas_veiculos = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
    colunas_veiculos_existentes = [col for col in colunas_veiculos if col in df_final.columns]
    
    if len(colunas_veiculos_existentes) > 0:
        colunas_id = [col for col in df_final.columns if col not in colunas_veiculos]
        df_final = df_final.melt(id_vars=colunas_id, value_vars=colunas_veiculos_existentes, var_name='Veículo', value_name='Total')
    
    log("📈 Processando volume (BUD)...")
    # Célula 10: Processar Volume BDG
    try:
        header_usado_vol_bdg = None
        sheet_usada_vol_bdg = 'Volume BDG'
        try:
            df_ke5z_volume = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name='Volume BDG', header=50)
            header_usado_vol_bdg = 50
        except ValueError:
            # Novo layout: header na primeira linha
            df_ke5z_volume = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name='Volume BDG', header=0)
            header_usado_vol_bdg = 0
        df_ke5z_volume = limpar_colunas_duplicadas(df_ke5z_volume)
        df_ke5z_volume = limpar_periodo_sufixos(df_ke5z_volume)
    except ValueError as e:
        if "Worksheet named 'Volume BDG' not found" in str(e):
            xl_file = pd.ExcelFile(config['CAMINHO_RATEIO'])
            guias_similares = [s for s in xl_file.sheet_names if 'volume' in s.lower() or 'bdg' in s.lower()]
            if guias_similares:
                try:
                    df_ke5z_volume = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name=guias_similares[0], header=50)
                    header_usado_vol_bdg = 50
                except ValueError:
                    df_ke5z_volume = pd.read_excel(config['CAMINHO_RATEIO'], sheet_name=guias_similares[0], header=0)
                    header_usado_vol_bdg = 0
                sheet_usada_vol_bdg = guias_similares[0]
                df_ke5z_volume = limpar_colunas_duplicadas(df_ke5z_volume)
                df_ke5z_volume = limpar_periodo_sufixos(df_ke5z_volume)
            else:
                raise
    
    df_ke5z_volume = df_ke5z_volume.dropna(axis=1, how='all')
    df_ke5z_volume = df_ke5z_volume.loc[:, [col for col in df_ke5z_volume.columns if (not (pd.isna(col) or str(col).strip() == ""))]]

    # Normalizar alias de coluna (governança: Volume BUD deve ter Veículo)
    df_ke5z_volume = _aplicar_alias_colunas(df_ke5z_volume, {'Veículo': ['Veículo', 'Veiculo']})

    # Governança: falhar cedo se não existir Veículo no Volume BDG
    if 'Veículo' not in df_ke5z_volume.columns:
        cols = ", ".join([str(c) for c in df_ke5z_volume.columns])
        raise ValueError(
            "❌ Aba 'Volume BDG' sem a coluna 'Veículo'. "
            f"Sheet lida: '{sheet_usada_vol_bdg}' | header={header_usado_vol_bdg}. "
            f"Colunas encontradas: {cols}"
        )
    
    # 🔧 CORREÇÃO CRÍTICA: Remover TODAS as colunas Unnamed: (colunas vazias do Excel)
    # Isso previne colunas vazias que causam duplicação ao consolidar
    colunas_unnamed = [col for col in df_ke5z_volume.columns if 'Unnamed:' in str(col)]
    if colunas_unnamed:
        log(f"⚠️ Removendo {len(colunas_unnamed)} colunas 'Unnamed:' vazias do Excel")
        df_ke5z_volume = df_ke5z_volume.drop(columns=colunas_unnamed)
    
    colunas_meses_encontradas = [col for col in df_ke5z_volume.columns if _mes_prefixo(col)]

    if not colunas_meses_encontradas:
        raise ValueError(
            "❌ Não encontrei colunas de meses na aba 'Volume BDG' (header=50). "
            "Verifique se as colunas Janeiro..Dezembro existem e se o header está correto."
        )
    
    df_vol = pd.melt(
        df_ke5z_volume,
        id_vars=[col for col in df_ke5z_volume.columns if col not in colunas_meses_encontradas],
        value_vars=colunas_meses_encontradas,
        var_name='Período',
        value_name='Volume'
    )
    
    df_vol['Período'] = df_vol['Período'].apply(lambda v: mapeamento_meses_prefixo.get(_mes_prefixo(v), str(v).strip().capitalize()))
    
    # 🚨 Regra do negócio: Volume BUD *sempre* precisa ter 'Veículo'.
    # Se não existir, isso é erro de extração e deve ser corrigido na fonte (Excel).
    _exigir_colunas(df_vol, ['Oficina', 'Veículo', 'Período', 'Volume'], "aba 'Volume BDG' após melt")
    df_vol['Volume'] = pd.to_numeric(df_vol['Volume'], errors='coerce').fillna(0)
    df_vol = df_vol[df_vol['Oficina'].notna() & df_vol['Período'].notna() & df_vol['Veículo'].notna()]
    
    # ═══ EXCLUIR oficinas inválidas ═══
    df_vol = filtrar_oficinas_validas(df_vol, "do Volume BDG ")

    # 🔧 Robustez: ignorar colunas extras e consolidar no grão correto (Oficina/Veículo/Período)
    df_vol = df_vol[['Oficina', 'Veículo', 'Período', 'Volume']].drop_duplicates()
    df_vol['Oficina'] = df_vol['Oficina'].astype(str).str.strip()
    df_vol['Veículo'] = df_vol['Veículo'].astype(str).str.strip()
    df_vol = df_vol.groupby(['Oficina', 'Veículo', 'Período'], as_index=False)['Volume'].sum()
    
    log("🔍 Aplicando filtros (BUD)...")
    # Célula 11: Filtrar Account
    df_final = df_final[df_final['Account'].notna() & (df_final['Account'] != 0) & (df_final['Account'] != 'TC Ext')]
    
    # Célula 11: Salvar Excel após filtro Account (df_final_cpu_BUD.xlsx)
    if 'CAMINHO_DF_FINAL_CPU_XLSX' in config:
        df_final.to_excel(config['CAMINHO_DF_FINAL_CPU_XLSX'], index=False)
        log(f"💾 Excel após filtro Account BUD salvo: {config['CAMINHO_DF_FINAL_CPU_XLSX']}")
    
    log("📊 Agrupando dados (BUD)...")
    # Célula 12: Agrupar Volume e fazer merge final
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
        df_KE5Z['Período'] = df_KE5Z['Período'].apply(
            lambda v: mapeamento_meses_prefixo.get(_mes_prefixo(v), str(v).strip().capitalize())
        )
    
    df_vol_group = df_vol.groupby(['Oficina', 'Período'], as_index=False)['Volume'].sum()
    
    df_ke5z_group = pd.merge(
        df_KE5Z.drop(columns=[col for col in df_KE5Z.columns if col.lower() == 'volume']),
        df_vol_group,
        on=['Oficina', 'Período'],
        how='left'
    )
    
    # Célula 12: Salvar Excel do df_ke5z_group BUD
    if 'CAMINHO_DF_KE5Z_GROUP_XLSX' in config:
        df_ke5z_group.to_excel(config['CAMINHO_DF_KE5Z_GROUP_XLSX'], index=False)
        log(f"💾 Excel df_ke5z_group BUD salvo: {config['CAMINHO_DF_KE5Z_GROUP_XLSX']}")
    
    log("✅ Processamento BUD concluído!")
    return df_final, df_vol, df_ke5z_group


def salvar_e_consolidar_bud(df_final: pd.DataFrame, df_vol: pd.DataFrame, df_ke5z_group: pd.DataFrame, 
                            config: Dict[str, any], progress_callback=None):
    """
    Salva os DataFrames BUD e consolida com histórico (Célula 13 do notebook BUD)
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)
    
    log("💾 Salvando arquivos BUD...")

    # Governança: Volume BUD deve conter 'Veículo'
    _exigir_colunas(df_vol, ['Oficina', 'Veículo', 'Período', 'Volume'], "df_vol (BUD) antes de salvar")
    
    # Adicionar coluna Ano
    if 'Ano' not in df_final.columns:
        df_final['Ano'] = config['ANO_ATUAL']
    if 'Ano' not in df_vol.columns:
        df_vol['Ano'] = config['ANO_ATUAL']
    if 'Ano' not in df_ke5z_group.columns:
        df_ke5z_group['Ano'] = config['ANO_ATUAL']
    
    # Normalizar e salvar
    df_final = normalizar_tipos_para_parquet(df_final)
    df_final.to_parquet(config['CAMINHO_DF_FINAL'])
    
    df_vol = normalizar_tipos_para_parquet(df_vol)
    if 'Volume' in df_vol.columns:
        df_vol['Volume'] = df_vol['Volume'].astype('float64')
    df_vol.to_parquet(config['CAMINHO_DF_VOL'])
    
    df_ke5z_group = normalizar_tipos_para_parquet(df_ke5z_group)
    df_ke5z_group.to_parquet(config['CAMINHO_DF_KE5Z_GROUP'])
    
    log("📊 Salvando arquivos Excel BUD...")
    # Célula 13: Salvar todos os arquivos Excel BUD
    if 'CAMINHO_DF_FINAL_XLSX' in config:
        df_final.to_excel(config['CAMINHO_DF_FINAL_XLSX'], index=False)
        log(f"   ✅ df_final_BUD.xlsx salvo")
    
    if 'CAMINHO_DF_VOL_XLSX' in config:
        df_vol.to_excel(config['CAMINHO_DF_VOL_XLSX'], index=False)
        log(f"   ✅ df_vol_BUD.xlsx salvo")
    
    if 'CAMINHO_DF_KE5Z_GROUP_XLSX' in config:
        df_ke5z_group.to_excel(config['CAMINHO_DF_KE5Z_GROUP_XLSX'], index=False)
        log(f"   ✅ df_ke5z_group_BUD.xlsx salvo")
    
    log("📚 Consolidando histórico BUD...")
    
    # Consolidar histórico
    pasta_dados = 'dados/TC_Ext'
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
            pasta_ano_bud = os.path.join(pasta_dados, str(ano), 'BUD')
            if nome_df == 'df_final':
                caminho_ano = os.path.join(pasta_ano_bud, 'df_final_BUD.parquet')
            elif nome_df == 'df_vol':
                caminho_ano = os.path.join(pasta_ano_bud, 'df_vol_BUD.parquet')
            elif nome_df == 'df_ke5z_group':
                caminho_ano = os.path.join(pasta_ano_bud, 'df_ke5z_group_BUD.parquet')
            else:
                continue
            
            if os.path.exists(caminho_ano):
                try:
                    df_ano = pd.read_parquet(caminho_ano)

                    # Governança: todos os anos de volume BUD precisam ter 'Veículo'
                    if nome_df == 'df_vol' and 'Veículo' not in df_ano.columns:
                        raise ValueError(
                            f"❌ ERRO NA EXTRAÇÃO: o volume BUD do ano {ano} não contém a coluna 'Veículo'. "
                            f"Arquivo: {caminho_ano}. Reextraia/corrija esse ano antes de consolidar."
                        )

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
                    
                    if 'Ano' not in df_ano.columns:
                        df_ano['Ano'] = ano
                    dfs_todos_anos.append(df_ano)
                except Exception as e:
                    log(f"⚠️ Erro ao carregar {caminho_ano}: {e}")
        
        if len(dfs_todos_anos) > 0:
            df_consolidado = pd.concat(dfs_todos_anos, ignore_index=True)
            df_consolidado = df_consolidado.drop_duplicates()
            df_consolidado = normalizar_tipos_para_parquet(df_consolidado)
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
    
    log("✅ Salvamento e consolidação BUD concluídos!")


def processar_completo_bud(ano: Optional[int] = None, continuar_sem_arquivos: bool = False, 
                          progress_callback=None) -> Dict[str, any]:
    """
    Executa o processamento completo de dados BUDGET
    
    Args:
        ano: Ano para processar. Se None, usa o ano atual
        continuar_sem_arquivos: Se True, continua mesmo sem arquivos necessários
        progress_callback: Função opcional para reportar progresso
    
    Returns:
        Dicionário com informações do processamento
    """
    config = configurar_ano_bud(ano, continuar_sem_arquivos)
    df_final, df_vol, df_ke5z_group = processar_dados_budget(config, progress_callback)
    salvar_e_consolidar_bud(df_final, df_vol, df_ke5z_group, config, progress_callback)
    
    return {
        'sucesso': True,
        'ano': config['ANO_ATUAL'],
        'df_final_linhas': len(df_final),
        'df_vol_linhas': len(df_vol),
        'df_ke5z_group_linhas': len(df_ke5z_group)
    }

