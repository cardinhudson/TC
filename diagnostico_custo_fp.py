"""
Diagnóstico: Custo FP — Excel (massa FP) × SCI (parquet)
=========================================================
Compara os valores de Custo FP calculados pelo SCI com os valores
das abas "massa FP - Actual" e "massa FP - BDG" do Excel de referência.

Saída: tabela mês a mês com desvios, decomposição por oficina e
       identificação das fontes de divergência.
"""

import sys
import os
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')

if hasattr(sys, '_MEIPASS'):
    _ROOT = sys._MEIPASS
else:
    _ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Importar utilitários do pipeline ──
from processamento_dados_veiculos_BUD import (
    _corrigir_colunas_mojibake,
    _detectar_colunas_meses,
    _normalizar_periodo,
)

ANO = 2026
PASTA_ANO = os.path.join(_ROOT, 'dados', 'TC_Principal', str(ANO))
PASTA_BUD = os.path.join(PASTA_ANO, 'BUD')
EXCEL_PATH = os.path.join(PASTA_ANO, 'Reporting veículos.xlsx')

SEPARADOR = "=" * 80


def fmt(v):
    """Formata número com separador de milhar."""
    return f"{v:>18,.2f}"


def pct(diff, ref):
    """Calcula percentual de diferença."""
    if ref == 0:
        return 0.0
    return abs(diff) / abs(ref) * 100


def status_icon(pct_diff):
    if pct_diff < 0.01:
        return "✅"
    elif pct_diff < 1.0:
        return "⚠️"
    else:
        return "❌"


# ══════════════════════════════════════════════════════════════
#  0. VERIFICAR EXISTÊNCIA DOS ARQUIVOS
# ══════════════════════════════════════════════════════════════
def verificar_arquivos():
    print(SEPARADOR)
    print("  0. VERIFICAÇÃO DE ARQUIVOS")
    print(SEPARADOR)

    arquivos = {
        'Excel': EXCEL_PATH,
        'Parquet Real': os.path.join(PASTA_ANO, 'df_principal.parquet'),
        'Parquet BUD': os.path.join(PASTA_BUD, 'df_principal_BUD.parquet'),
    }

    ok = True
    for nome, caminho in arquivos.items():
        existe = os.path.exists(caminho)
        ico = "✅" if existe else "❌"
        print(f"   {ico} {nome}: {caminho}")
        if not existe:
            ok = False

    # Verificar abas do Excel
    if os.path.exists(EXCEL_PATH):
        xls = pd.ExcelFile(EXCEL_PATH)
        abas = xls.sheet_names
        print(f"\n   Abas disponíveis no Excel ({len(abas)}):")
        for aba in abas:
            print(f"     • {aba}")

        abas_fp = [a for a in abas if 'massa fp' in a.lower() or 'massa_fp' in a.lower()]
        if abas_fp:
            print(f"\n   📌 Abas 'massa FP' encontradas: {abas_fp}")
        else:
            print(f"\n   ⚠️ Nenhuma aba 'massa FP' encontrada no Excel!")
            print(f"      Abas com 'massa': {[a for a in abas if 'massa' in a.lower()]}")

    return ok


# ══════════════════════════════════════════════════════════════
#  1. LER ABA "massa FP" DO EXCEL (Actual e/ou BDG)
# ══════════════════════════════════════════════════════════════
def ler_excel_massa_fp(aba_nome: str) -> pd.DataFrame:
    """
    Lê uma aba 'massa FP - ...' do Excel.
    Faz melt para formato longo: Oficina, Período, Custo FP (Excel).
    """
    print(f"\n   Lendo aba '{aba_nome}'...")
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=aba_nome)
    except ValueError:
        print(f"   ❌ Aba '{aba_nome}' não encontrada!")
        return pd.DataFrame()

    df = _corrigir_colunas_mojibake(df)

    # Mostrar estrutura
    print(f"   Colunas: {list(df.columns)}")
    print(f"   Shape: {df.shape}")

    colunas_meses = _detectar_colunas_meses(df)
    if not colunas_meses:
        # Tentar detectar manualmente
        _meses_pt = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
        colunas_meses = [c for c in df.columns if any(
            str(c).lower().strip().startswith(m) for m in _meses_pt
        )]
        if not colunas_meses:
            print(f"   ❌ Sem colunas de meses detectadas na aba '{aba_nome}'")
            return pd.DataFrame()

    print(f"   Colunas de meses: {colunas_meses}")

    # Remover Ano se existir
    if 'Ano' in df.columns:
        df = df.drop(columns=['Ano'])

    # Identificar coluna de Oficina
    col_oficina = None
    for c in df.columns:
        if str(c).lower().strip() == 'oficina':
            col_oficina = c
            break
    
    if col_oficina is None:
        # Sem oficina — somar total
        print(f"   ⚠️ Coluna 'Oficina' não encontrada — usando total")
        soma_por_mes = {}
        for mc in colunas_meses:
            periodo = _normalizar_periodo(mc)
            soma_por_mes[periodo] = pd.to_numeric(df[mc], errors='coerce').fillna(0).sum()
        df_result = pd.DataFrame([
            {'Período': p, 'FP_Excel': v} for p, v in soma_por_mes.items()
        ])
        return df_result

    # Colunas dimensionais (não são meses)
    colunas_dim = [c for c in df.columns if c not in colunas_meses]

    df_melt = df.melt(
        id_vars=colunas_dim,
        value_vars=colunas_meses,
        var_name='Período',
        value_name='FP_Excel'
    )
    df_melt['Período'] = df_melt['Período'].apply(_normalizar_periodo)
    df_melt['FP_Excel'] = pd.to_numeric(df_melt['FP_Excel'], errors='coerce').fillna(0)

    # Renomear coluna oficina
    if col_oficina != 'Oficina':
        df_melt = df_melt.rename(columns={col_oficina: 'Oficina'})

    # Remover linhas sem Oficina
    df_melt = df_melt[df_melt['Oficina'].notna() & (df_melt['Oficina'].astype(str).str.strip() != '')]

    print(f"   ✅ {len(df_melt):,} linhas lidas")
    print(f"   Oficinas: {sorted(df_melt['Oficina'].unique())}")
    print(f"   Total FP Excel: R$ {df_melt['FP_Excel'].sum():,.2f}")

    return df_melt


# ══════════════════════════════════════════════════════════════
#  2. LER PARQUET SCI
# ══════════════════════════════════════════════════════════════
def ler_parquet_principal(tipo: str = 'real') -> pd.DataFrame:
    """
    Lê df_principal.parquet (Real) ou df_principal_BUD.parquet (BUD).
    """
    if tipo == 'real':
        path = os.path.join(PASTA_ANO, 'df_principal.parquet')
    else:
        path = os.path.join(PASTA_BUD, 'df_principal_BUD.parquet')

    print(f"\n   Lendo parquet {tipo}: {os.path.basename(path)}...")

    if not os.path.exists(path):
        print(f"   ❌ Arquivo não encontrado: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)

    colunas_chave = ['Oficina', 'Período', 'Despesa Primaria', 'Custo FA', 'Custo FP']
    colunas_presentes = [c for c in colunas_chave if c in df.columns]
    print(f"   Colunas: {list(df.columns)}")
    print(f"   Shape: {df.shape}")
    print(f"   Colunas-chave presentes: {colunas_presentes}")

    if '_fonte_redis' in df.columns:
        n_redis = (df['_fonte_redis'] == True).sum()
        n_sapiens = (df['_fonte_redis'] != True).sum()
        print(f"   Linhas Redis: {n_redis:,} | Sapiens: {n_sapiens:,}")

    for col in ['Despesa Primaria', 'Custo FA', 'Custo FP']:
        if col in df.columns:
            print(f"   {col}: R$ {df[col].sum():,.2f}")

    print(f"   Oficinas: {sorted(df['Oficina'].unique()) if 'Oficina' in df.columns else 'N/A'}")

    return df


# ══════════════════════════════════════════════════════════════
#  3. COMPARAÇÃO GLOBAL MÊS A MÊS
# ══════════════════════════════════════════════════════════════
def comparar_global(df_excel: pd.DataFrame, df_parquet: pd.DataFrame, label: str):
    """
    Compara Custo FP do Excel × Parquet por período.
    """
    print(f"\n{SEPARADOR}")
    print(f"  COMPARAÇÃO GLOBAL: {label}")
    print(SEPARADOR)

    if df_excel.empty or df_parquet.empty:
        print("   ❌ Dados insuficientes para comparação")
        return

    # Agregar Excel por período
    if 'Oficina' in df_excel.columns:
        excel_per = df_excel.groupby('Período', as_index=False)['FP_Excel'].sum()
    else:
        excel_per = df_excel.copy()

    # Agregar Parquet por período
    pq_per = df_parquet.groupby('Período', as_index=False)['Custo FP'].sum()
    pq_per = pq_per.rename(columns={'Custo FP': 'FP_SCI'})

    # Merge
    comp = pd.merge(excel_per, pq_per, on='Período', how='outer').fillna(0)
    comp['Diff'] = comp['FP_SCI'] - comp['FP_Excel']
    comp['%Diff'] = comp.apply(lambda r: pct(r['Diff'], r['FP_Excel']), axis=1)
    comp['Status'] = comp['%Diff'].apply(status_icon)

    # Ordenar por período
    try:
        comp['_sort'] = comp['Período'].apply(lambda p: int(p.split('/')[0]) if '/' in str(p) else 0)
        comp = comp.sort_values('_sort').drop(columns=['_sort'])
    except:
        pass

    # Imprimir
    print(f"\n   {'Período':<12} {'Excel FP':>18} {'SCI FP':>18} {'Diferença':>18} {'%Diff':>8}  Status")
    print(f"   {'-'*12} {'-'*18} {'-'*18} {'-'*18} {'-'*8}  ------")

    for _, row in comp.iterrows():
        print(f"   {row['Período']:<12} {fmt(row['FP_Excel'])} {fmt(row['FP_SCI'])} {fmt(row['Diff'])} {row['%Diff']:>7.2f}%  {row['Status']}")

    # Totais
    total_excel = comp['FP_Excel'].sum()
    total_sci = comp['FP_SCI'].sum()
    total_diff = total_sci - total_excel
    total_pct = pct(total_diff, total_excel)
    print(f"   {'-'*12} {'-'*18} {'-'*18} {'-'*18} {'-'*8}  ------")
    print(f"   {'TOTAL':<12} {fmt(total_excel)} {fmt(total_sci)} {fmt(total_diff)} {total_pct:>7.2f}%  {status_icon(total_pct)}")

    return comp


# ══════════════════════════════════════════════════════════════
#  4. DECOMPOSIÇÃO POR OFICINA
# ══════════════════════════════════════════════════════════════
def comparar_por_oficina(df_excel: pd.DataFrame, df_parquet: pd.DataFrame, label: str):
    """
    Compara Custo FP por oficina (total de todos os períodos).
    Identifica oficinas presentes em um mas não no outro.
    """
    print(f"\n{SEPARADOR}")
    print(f"  DECOMPOSIÇÃO POR OFICINA: {label}")
    print(SEPARADOR)

    if df_excel.empty or df_parquet.empty:
        print("   ❌ Dados insuficientes")
        return

    if 'Oficina' not in df_excel.columns:
        print("   ⚠️ Excel sem coluna Oficina — pulando decomposição")
        return

    # Agregar por oficina
    excel_of = df_excel.groupby('Oficina', as_index=False)['FP_Excel'].sum()
    pq_of = df_parquet.groupby('Oficina', as_index=False)['Custo FP'].sum()
    pq_of = pq_of.rename(columns={'Custo FP': 'FP_SCI'})

    comp = pd.merge(excel_of, pq_of, on='Oficina', how='outer').fillna(0)
    comp['Diff'] = comp['FP_SCI'] - comp['FP_Excel']
    comp['%Diff'] = comp.apply(lambda r: pct(r['Diff'], r['FP_Excel']), axis=1)
    comp['Status'] = comp['%Diff'].apply(status_icon)
    comp = comp.sort_values('Oficina')

    print(f"\n   {'Oficina':<12} {'Excel FP':>18} {'SCI FP':>18} {'Diferença':>18} {'%Diff':>8}  Status  Nota")
    print(f"   {'-'*12} {'-'*18} {'-'*18} {'-'*18} {'-'*8}  ------  ----")

    for _, row in comp.iterrows():
        nota = ""
        if row['FP_Excel'] == 0 and row['FP_SCI'] != 0:
            nota = "⚠️ SÓ NO SCI"
        elif row['FP_SCI'] == 0 and row['FP_Excel'] != 0:
            nota = "⚠️ SÓ NO EXCEL"
        print(f"   {row['Oficina']:<12} {fmt(row['FP_Excel'])} {fmt(row['FP_SCI'])} {fmt(row['Diff'])} {row['%Diff']:>7.2f}%  {row['Status']}  {nota}")

    # Oficinas exclusivas
    of_excel = set(df_excel['Oficina'].unique())
    of_sci = set(df_parquet['Oficina'].unique())
    so_excel = of_excel - of_sci
    so_sci = of_sci - of_excel

    if so_excel:
        print(f"\n   ⚠️ Oficinas SOMENTE no Excel: {sorted(so_excel)}")
    if so_sci:
        print(f"   ⚠️ Oficinas SOMENTE no SCI:   {sorted(so_sci)}")
    if not so_excel and not so_sci:
        print(f"\n   ✅ Mesmas oficinas em ambas as fontes")


# ══════════════════════════════════════════════════════════════
#  5. DECOMPOSIÇÃO POR COMPONENTE (DP, Redis, FA, FP)
# ══════════════════════════════════════════════════════════════
def decompor_componentes(df_parquet: pd.DataFrame, label: str):
    """
    Mostra DP, Redis, Custo FA, Custo FP do parquet.
    """
    print(f"\n{SEPARADOR}")
    print(f"  COMPONENTES DO PARQUET: {label}")
    print(SEPARADOR)

    if df_parquet.empty:
        print("   ❌ Parquet vazio")
        return

    # Separar Redis e Sapiens
    if '_fonte_redis' in df_parquet.columns:
        df_redis = df_parquet[df_parquet['_fonte_redis'] == True]
        df_sapiens = df_parquet[df_parquet['_fonte_redis'] != True]
    else:
        df_redis = pd.DataFrame()
        df_sapiens = df_parquet

    print(f"\n   Componente                     Total")
    print(f"   {'─'*35} {'─'*20}")
    print(f"   DP (Sapiens)               {fmt(df_sapiens['Despesa Primaria'].sum() if 'Despesa Primaria' in df_sapiens.columns else 0)}")
    print(f"   DP (Redis, sinal invertido) {fmt(df_redis['Despesa Primaria'].sum() if len(df_redis) > 0 and 'Despesa Primaria' in df_redis.columns else 0)}")
    print(f"   DP Total                    {fmt(df_parquet['Despesa Primaria'].sum() if 'Despesa Primaria' in df_parquet.columns else 0)}")
    print(f"   Custo FA                    {fmt(df_parquet['Custo FA'].sum() if 'Custo FA' in df_parquet.columns else 0)}")
    print(f"   Custo FP                    {fmt(df_parquet['Custo FP'].sum() if 'Custo FP' in df_parquet.columns else 0)}")

    # Por período
    if all(c in df_parquet.columns for c in ['Período', 'Despesa Primaria', 'Custo FA', 'Custo FP']):
        print(f"\n   {'Período':<12} {'DP Total':>18} {'Custo FA':>18} {'Custo FP':>18} {'DP-FA-FP':>14}")
        print(f"   {'-'*12} {'-'*18} {'-'*18} {'-'*18} {'-'*14}")

        per_agg = df_parquet.groupby('Período', as_index=False).agg({
            'Despesa Primaria': 'sum',
            'Custo FA': 'sum',
            'Custo FP': 'sum'
        })
        try:
            per_agg['_sort'] = per_agg['Período'].apply(lambda p: int(p.split('/')[0]) if '/' in str(p) else 0)
            per_agg = per_agg.sort_values('_sort').drop(columns=['_sort'])
        except:
            pass

        for _, row in per_agg.iterrows():
            prova = row['Despesa Primaria'] - row['Custo FA'] - row['Custo FP']
            print(f"   {row['Período']:<12} {fmt(row['Despesa Primaria'])} {fmt(row['Custo FA'])} {fmt(row['Custo FP'])} {prova:>14.2f}")


# ══════════════════════════════════════════════════════════════
#  6. VERIFICAR REDIS NO PARQUET vs EXCEL
# ══════════════════════════════════════════════════════════════
def verificar_redis(df_parquet: pd.DataFrame, label: str):
    """
    Compara Redis no parquet com aba 'massa - REDIS' do Excel.
    """
    print(f"\n{SEPARADOR}")
    print(f"  VERIFICAÇÃO REDIS: {label}")
    print(SEPARADOR)

    # Excel
    try:
        df_excel_redis = pd.read_excel(EXCEL_PATH, sheet_name='massa - REDIS')
        df_excel_redis = _corrigir_colunas_mojibake(df_excel_redis)
        meses = _detectar_colunas_meses(df_excel_redis)
        if not meses:
            _meses_pt = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
            meses = [c for c in df_excel_redis.columns if any(str(c).lower().strip().startswith(m) for m in _meses_pt)]

        soma_excel_redis = sum(pd.to_numeric(df_excel_redis[mc], errors='coerce').fillna(0).sum() for mc in meses)
        print(f"   Excel 'massa - REDIS' total: R$ {soma_excel_redis:,.2f}")
    except Exception as e:
        print(f"   ❌ Erro lendo aba massa - REDIS: {e}")
        return

    # Parquet
    if '_fonte_redis' in df_parquet.columns:
        df_redis_pq = df_parquet[df_parquet['_fonte_redis'] == True]
        soma_pq_redis = df_redis_pq['Despesa Primaria'].sum() if 'Despesa Primaria' in df_redis_pq.columns else 0
        # Redis no parquet é negativo (invertido)
        print(f"   Parquet Redis DP (invertido): R$ {soma_pq_redis:,.2f}")
        print(f"   Parquet Redis DP (abs):       R$ {abs(soma_pq_redis):,.2f}")

        diff = abs(soma_excel_redis - abs(soma_pq_redis))
        p = pct(diff, soma_excel_redis)
        print(f"   Diferença: R$ {diff:,.2f} ({p:.4f}%) {status_icon(p)}")
    else:
        print(f"   ⚠️ Coluna '_fonte_redis' não encontrada no parquet")


# ══════════════════════════════════════════════════════════════
#  7. COMPARAR SAPIENS PARQUET vs EXCEL
# ══════════════════════════════════════════════════════════════
def verificar_sapiens(df_parquet: pd.DataFrame, label: str):
    """
    Compara DP (Sapiens) do parquet com aba 'Sapiens' do Excel.
    """
    print(f"\n{SEPARADOR}")
    print(f"  VERIFICAÇÃO SAPIENS: {label}")
    print(SEPARADOR)

    # Excel
    try:
        df_sap = pd.read_excel(EXCEL_PATH, sheet_name='Sapiens', header=1)

        # Aplicar mesma lógica do processamento
        # Renomear Valor → Despesa Primaria
        if 'Valor' in df_sap.columns:
            df_sap = df_sap.rename(columns={'Valor': 'DP_Excel'})
        elif 'Despesa Primaria' in df_sap.columns:
            df_sap = df_sap.rename(columns={'Despesa Primaria': 'DP_Excel'})
        else:
            print(f"   ❌ Coluna 'Valor' não encontrada na aba Sapiens")
            return

        df_sap['DP_Excel'] = pd.to_numeric(df_sap['DP_Excel'], errors='coerce').fillna(0)

        # Filtrar Account != Redis
        if 'Account' in df_sap.columns:
            n_redis_excel = (df_sap['Account'] == 'Redis').sum()
            dp_redis_excel = df_sap[df_sap['Account'] == 'Redis']['DP_Excel'].sum()
            print(f"   Linhas Redis no Sapiens: {n_redis_excel} (DP = R$ {dp_redis_excel:,.2f})")
            df_sap = df_sap[df_sap['Account'] != 'Redis']

        # Filtrar DP = 0
        df_sap = df_sap[df_sap['DP_Excel'] != 0]

        # Filtrar oficinas inválidas
        if 'Oficina' in df_sap.columns:
            from processamento_dados_veiculos import OFICINAS_INVALIDAS
            df_sap = df_sap[~df_sap['Oficina'].isin(OFICINAS_INVALIDAS)]

            # Mostrar oficinas do Excel
            oficinas_excel = sorted(df_sap['Oficina'].dropna().unique().tolist())
            print(f"   Oficinas no Sapiens (pós-filtro): {oficinas_excel}")

        soma_excel_sap = df_sap['DP_Excel'].sum()
        print(f"   Excel Sapiens DP (sem Redis, sem inv.): R$ {soma_excel_sap:,.2f}")

    except Exception as e:
        print(f"   ❌ Erro lendo aba Sapiens: {e}")
        import traceback
        traceback.print_exc()
        return

    # Parquet (sem Redis)
    if '_fonte_redis' in df_parquet.columns:
        df_sap_pq = df_parquet[df_parquet['_fonte_redis'] != True]
    else:
        df_sap_pq = df_parquet

    soma_pq_sap = df_sap_pq['Despesa Primaria'].sum() if 'Despesa Primaria' in df_sap_pq.columns else 0
    print(f"   Parquet Sapiens DP:                      R$ {soma_pq_sap:,.2f}")

    diff = abs(soma_excel_sap - soma_pq_sap)
    p = pct(diff, soma_excel_sap)
    print(f"   Diferença: R$ {diff:,.2f} ({p:.4f}%) {status_icon(p)}")

    # Oficinas do parquet
    if 'Oficina' in df_sap_pq.columns:
        oficinas_pq = sorted(df_sap_pq['Oficina'].dropna().unique().tolist())
        print(f"   Oficinas no parquet: {oficinas_pq}")

        # Diferenças de oficinas
        of_excel_set = set(oficinas_excel) if 'oficinas_excel' in dir() else set()
        of_pq_set = set(oficinas_pq)

        if of_excel_set:
            so_excel = of_excel_set - of_pq_set
            so_pq = of_pq_set - of_excel_set
            if so_excel:
                dp_somente_excel = df_sap[df_sap['Oficina'].isin(so_excel)]['DP_Excel'].sum()
                print(f"   ⚠️ Oficinas SOMENTE no Excel Sapiens: {sorted(so_excel)} (DP = R$ {dp_somente_excel:,.2f})")
            if so_pq:
                dp_somente_pq = df_sap_pq[df_sap_pq['Oficina'].isin(so_pq)]['Despesa Primaria'].sum()
                print(f"   ⚠️ Oficinas SOMENTE no Parquet: {sorted(so_pq)} (DP = R$ {dp_somente_pq:,.2f})")


# ══════════════════════════════════════════════════════════════
#  8. CRUZAMENTO DETALHADO POR OFICINA × PERÍODO
# ══════════════════════════════════════════════════════════════
def cruzamento_oficina_periodo(df_excel: pd.DataFrame, df_parquet: pd.DataFrame, label: str):
    """
    Cruzamento detalhado por Oficina × Período para encontrar exatamente onde está a divergência.
    """
    print(f"\n{SEPARADOR}")
    print(f"  CRUZAMENTO OFICINA × PERÍODO: {label}")
    print(SEPARADOR)

    if df_excel.empty or df_parquet.empty:
        print("   ❌ Dados insuficientes")
        return

    if 'Oficina' not in df_excel.columns:
        print("   ⚠️ Excel sem coluna Oficina — pulando cruzamento detalhado")
        return

    # Agregar
    excel_op = df_excel.groupby(['Oficina', 'Período'], as_index=False)['FP_Excel'].sum()
    pq_op = df_parquet.groupby(['Oficina', 'Período'], as_index=False)['Custo FP'].sum()
    pq_op = pq_op.rename(columns={'Custo FP': 'FP_SCI'})

    comp = pd.merge(excel_op, pq_op, on=['Oficina', 'Período'], how='outer').fillna(0)
    comp['Diff'] = comp['FP_SCI'] - comp['FP_Excel']
    comp['%Diff'] = comp.apply(lambda r: pct(r['Diff'], r['FP_Excel']), axis=1)

    # Filtrar apenas divergências significativas (> 0.01%)
    divergencias = comp[comp['%Diff'] > 0.01].sort_values('%Diff', ascending=False)

    if divergencias.empty:
        print("   ✅ Nenhuma divergência significativa por Oficina × Período")
        return

    print(f"\n   Top divergências ({len(divergencias)} encontradas, mostrando até 30):")
    print(f"   {'Oficina':<10} {'Período':<12} {'Excel FP':>16} {'SCI FP':>16} {'Diferença':>16} {'%Diff':>8}")
    print(f"   {'-'*10} {'-'*12} {'-'*16} {'-'*16} {'-'*16} {'-'*8}")

    for _, row in divergencias.head(30).iterrows():
        print(f"   {row['Oficina']:<10} {row['Período']:<12} {row['FP_Excel']:>16,.2f} {row['FP_SCI']:>16,.2f} {row['Diff']:>16,.2f} {row['%Diff']:>7.2f}%")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
def main():
    print("\n" + SEPARADOR)
    print("  DIAGNÓSTICO CUSTO FP — Excel (massa FP) × SCI (parquet)")
    print(f"  Ano: {ANO}")
    print(f"  Data: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEPARADOR)

    # 0. Verificar arquivos
    ok = verificar_arquivos()
    if not ok:
        print("\n❌ Arquivos faltando — abortando diagnóstico")
        return

    # ══════════════════════════════════════════════════════════
    #  REAL (Actual)
    # ══════════════════════════════════════════════════════════
    print(f"\n{'█'*80}")
    print(f"  ANÁLISE REAL (Actual)")
    print(f"{'█'*80}")

    df_excel_real = ler_excel_massa_fp('massa FP - Actual')
    df_pq_real = ler_parquet_principal('real')

    if not df_excel_real.empty and not df_pq_real.empty:
        verificar_sapiens(df_pq_real, "Real")
        verificar_redis(df_pq_real, "Real")
        decompor_componentes(df_pq_real, "Real")
        comparar_global(df_excel_real, df_pq_real, "Real (Custo FP: Excel × SCI)")
        comparar_por_oficina(df_excel_real, df_pq_real, "Real")
        cruzamento_oficina_periodo(df_excel_real, df_pq_real, "Real")
    else:
        print("\n   ⚠️ Não foi possível fazer comparação Real (dados insuficientes)")

    # ══════════════════════════════════════════════════════════
    #  BUDGET (BDG)
    # ══════════════════════════════════════════════════════════
    print(f"\n{'█'*80}")
    print(f"  ANÁLISE BUDGET (BDG)")
    print(f"{'█'*80}")

    df_excel_bud = ler_excel_massa_fp('massa FP - BDG')
    df_pq_bud = ler_parquet_principal('bud')

    if not df_excel_bud.empty and not df_pq_bud.empty:
        decompor_componentes(df_pq_bud, "Budget")
        comparar_global(df_excel_bud, df_pq_bud, "Budget (Custo FP: Excel × SCI)")
        comparar_por_oficina(df_excel_bud, df_pq_bud, "Budget")
        cruzamento_oficina_periodo(df_excel_bud, df_pq_bud, "Budget")
    else:
        print("\n   ⚠️ Não foi possível fazer comparação Budget (dados insuficientes)")

    print(f"\n{SEPARADOR}")
    print("  DIAGNÓSTICO CONCLUÍDO")
    print(SEPARADOR)


if __name__ == '__main__':
    main()
