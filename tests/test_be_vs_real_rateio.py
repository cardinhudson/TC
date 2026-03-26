"""
Diagnostico DEFINITIVO: recalcular rateio por veiculo do zero para Jan/Fev
usando exatamente a mesma logica do Real (fases 9, 13, 14) e comparar com
o arquivo gerado pelo BE e com o Real.
"""
import pandas as pd
import numpy as np
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'tc_principal'))

DADOS = os.path.join(ROOT, 'dados', 'TC_Principal')
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 220)
pd.set_option('display.float_format', '{:,.2f}'.format)


def diagnostico_completo():
    # ============================================================
    # CARREGAR TODOS OS DADOS
    # ============================================================
    print("=" * 80)
    print("CARREGANDO DADOS")
    print("=" * 80)

    # Real - resultado final por veiculo
    real_veic = pd.read_parquet(os.path.join(DADOS, '2026', 'df_veiculos_custo_fp.parquet'))
    # Real - principal (sem veiculo)
    real_principal = pd.read_parquet(os.path.join(DADOS, '2026', 'df_principal.parquet'))
    # Percentuais de rateio
    pct = pd.read_parquet(os.path.join(DADOS, '2026', 'df_veiculos_percentual_rateio.parquet'))
    # D&A dedicado por veiculo
    dea = pd.read_parquet(os.path.join(DADOS, '2026', 'df_dea_dedicado.parquet'))
    # Forecast completo (pre-rateio)
    fc = pd.read_parquet(os.path.join(DADOS, 'Forecast', 'forecast_completo.parquet'))
    # Forecast com veiculo (resultado do rateio BE)
    be_veic = pd.read_parquet(os.path.join(DADOS, 'Forecast', 'forecast_veiculos_custo_fp.parquet'))

    print(f"Real principal: {len(real_principal):,} linhas")
    print(f"Real veiculos: {len(real_veic):,} linhas")
    print(f"Percentuais: {len(pct):,} linhas")
    print(f"D&A dedicado: {len(dea):,} linhas")
    print(f"Forecast completo: {len(fc):,} linhas")
    print(f"BE veiculos: {len(be_veic):,} linhas")

    # ============================================================
    # ETAPA 1: Comparar dados de ENTRADA (pre-rateio)
    # Para Jan/Fev, Real principal vs forecast_completo devem ser iguais
    # ============================================================
    print("\n" + "=" * 80)
    print("ETAPA 1: DADOS DE ENTRADA (PRE-RATEIO) - Jan/Fev")
    print("=" * 80)

    meses = ['Janeiro', 'Fevereiro']

    real_jf = real_principal[real_principal['Periodo'].isin(meses)].copy() if 'Periodo' in real_principal.columns else real_principal[real_principal['Periodo'].isin(meses)].copy() if 'Periodo' in real_principal.columns else pd.DataFrame()
    if real_jf.empty:
        col_per = [c for c in real_principal.columns if 'per' in c.lower() or 'Per' in c][0]
        real_jf = real_principal[real_principal[col_per].isin(meses)]
        fc_jf = fc[fc[col_per].isin(meses)]
    else:
        col_per = 'Periodo'
        fc_jf = fc[fc[col_per].isin(meses)]

    print(f"\nReal principal Jan/Fev: {len(real_jf):,} linhas")
    print(f"Forecast completo Jan/Fev: {len(fc_jf):,} linhas")
    print(f"Numero de linhas diferente: {len(real_jf) != len(fc_jf)}")

    for col in ['Custo FP', 'FP sem Dedicada', 'D&A dedicado']:
        if col in real_jf.columns and col in fc_jf.columns:
            r = real_jf[col].sum()
            f = fc_jf[col].sum()
            d = f - r
            print(f"  {col}: Real={r:,.2f} | FC={f:,.2f} | Diff={d:,.2f}")
        elif col in real_jf.columns:
            print(f"  {col}: Real={real_jf[col].sum():,.2f} | FC=COLUNA AUSENTE")
        elif col in fc_jf.columns:
            print(f"  {col}: Real=COLUNA AUSENTE | FC={fc_jf[col].sum():,.2f}")

    # ============================================================
    # ETAPA 2: Simular rateio IDENTICO ao Real
    # Pegar os dados do forecast_completo para Jan/Fev
    # Aplicar fase 9 -> fase 13 -> fase 14
    # ============================================================
    print("\n" + "=" * 80)
    print("ETAPA 2: SIMULACAO DO RATEIO (usando dados forecast_completo Jan/Fev)")
    print("=" * 80)

    col_p = 'Periodo' if 'Periodo' in fc.columns else [c for c in fc.columns if 'per' in c.lower() or 'Per' in c][0]

    # --- FASE 9: FP sem Dedicada ---
    # Se forecast_completo ja tem FP sem Dedicada, verificar se bate
    if 'FP sem Dedicada' in fc.columns:
        print(f"\nforecast_completo JA TEM 'FP sem Dedicada'")
        fc_jf2 = fc[fc[col_p].isin(meses)].copy()
        print(f"  FP sem Dedicada (FC Jan/Fev): {fc_jf2['FP sem Dedicada'].sum():,.2f}")
        if 'D&A dedicado' in fc.columns:
            print(f"  D&A dedicado (FC Jan/Fev): {fc_jf2['D&A dedicado'].sum():,.2f}")
            print(f"  Custo FP (FC Jan/Fev): {fc_jf2['Custo FP'].sum():,.2f}")
            check = fc_jf2['FP sem Dedicada'].sum() + fc_jf2['D&A dedicado'].sum()
            print(f"  FP sem Ded + D&A = {check:,.2f} (deve = Custo FP {fc_jf2['Custo FP'].sum():,.2f})")
    else:
        print(f"\nforecast_completo NAO TEM 'FP sem Dedicada' - seria calculada")

    # --- FASE 13: Rateio por veiculo ---
    print(f"\n--- FASE 13: Merge com percentuais ---")
    # Usar forecast_completo Jan/Fev como entrada
    df_entrada = fc[fc[col_p].isin(meses)].copy()

    # Dropar colunas que serao recalculadas (como o fix faz)
    colunas_dropar = ['Veiculo', 'Percentual', 'Custo FP Veiculo', 'Custo Rateado', 'D&A dedicado']
    col_v = 'Veiculo' if 'Veiculo' in df_entrada.columns else 'Veiculo'
    # Tentar com acentos
    for c_try in ['Veiculo', 'Veículo']:
        if c_try in df_entrada.columns:
            col_v = c_try
    colunas_dropar_real = [c for c in ['Veículo', 'Percentual', 'Custo FP Veiculo', 'Custo Rateado', 'D&A dedicado'] if c in df_entrada.columns]
    df_limpo = df_entrada.drop(columns=colunas_dropar_real, errors='ignore')

    print(f"  Linhas entrada: {len(df_limpo):,}")
    print(f"  Colunas dropadas: {colunas_dropar_real}")
    print(f"  FP sem Dedicada presente: {'FP sem Dedicada' in df_limpo.columns}")

    pct_cols = pct[['Oficina', 'Veículo', 'Período', 'Percentual']].copy()
    df_merged = pd.merge(df_limpo, pct_cols, on=['Oficina', 'Período'], how='left')

    mask_sem = df_merged['Veículo'].isna()
    n_sem = mask_sem.sum()
    n_com = (~mask_sem).sum()
    print(f"  Apos merge: {len(df_merged):,} linhas (com veiculo: {n_com:,}, sem: {n_sem:,})")

    if n_sem > 0:
        # Aplicar fallback identico ao Real fase13
        df_com = df_merged[~mask_sem].copy()
        df_sem = df_merged[mask_sem].drop(columns=['Veículo', 'Percentual'], errors='ignore')
        dist_periodo = pct_cols.groupby(['Período', 'Veículo'])['Percentual'].mean().reset_index()
        soma_per = dist_periodo.groupby('Período')['Percentual'].transform('sum')
        dist_periodo['Percentual'] = dist_periodo['Percentual'] / soma_per.replace(0, 1)
        df_sem_exp = pd.merge(df_sem, dist_periodo, on='Período', how='left')
        mask_still = df_sem_exp['Veículo'].isna()
        if mask_still.any():
            veiculos_u = pct_cols['Veículo'].dropna().unique()
            n_v = max(1, len(veiculos_u))
            orfas = df_sem_exp[mask_still].drop(columns=['Veículo', 'Percentual'], errors='ignore')
            exps = []
            for v in veiculos_u:
                t = orfas.copy(); t['Veículo'] = v; t['Percentual'] = 1.0 / n_v; exps.append(t)
            df_sem_exp = pd.concat([df_sem_exp[~mask_still]] + exps, ignore_index=True)
        df_merged = pd.concat([df_com, df_sem_exp], ignore_index=True)
        print(f"  Apos fallback: {len(df_merged):,} linhas")

    df_merged['Percentual'] = df_merged['Percentual'].fillna(0)
    df_merged['Custo Rateado'] = df_merged['FP sem Dedicada'] * df_merged['Percentual']

    print(f"  Custo Rateado total: {df_merged['Custo Rateado'].sum():,.2f}")
    print(f"  FP sem Dedicada total entrada: {df_entrada['FP sem Dedicada'].sum():,.2f}")
    diff_fase13 = abs(df_merged['Custo Rateado'].sum() - df_entrada['FP sem Dedicada'].sum())
    print(f"  Diff fechamento fase 13: {diff_fase13:,.2f}")

    # --- FASE 14: D&A dedicado por veiculo ---
    print(f"\n--- FASE 14: D&A dedicado por veiculo ---")
    print(f"  D&A parquet: {len(dea):,} linhas, colunas: {sorted(dea.columns.tolist())}")
    print(f"  D&A tem Veiculo: {'Veículo' in dea.columns}")

    if 'Veículo' in dea.columns:
        cols_merge_dea = ['Oficina', 'Veículo', 'Account', 'Período']
        cols_merge_dea = [c for c in cols_merge_dea if c in dea.columns and c in df_merged.columns]
        print(f"  Merge keys D&A: {cols_merge_dea}")

        dea_jf = dea[dea['Período'].isin(meses)]
        print(f"  D&A Jan/Fev: {len(dea_jf):,} linhas, total: {dea_jf['D&A dedicado'].sum():,.2f}")

        dea_agg = dea_jf.groupby(cols_merge_dea, as_index=False)['D&A dedicado'].sum()
        dea_agg = dea_agg.rename(columns={'D&A dedicado': '_dea_veiculo'})

        df_final = pd.merge(df_merged, dea_agg, on=cols_merge_dea, how='left')
        df_final['_dea_veiculo'] = df_final['_dea_veiculo'].fillna(0)

        _n_rows = df_final.groupby(cols_merge_dea)['Custo Rateado'].transform('count')
        df_final['D&A dedicado'] = df_final['_dea_veiculo'] / _n_rows.replace(0, 1)
        df_final.drop(columns=['_dea_veiculo'], inplace=True, errors='ignore')
    else:
        df_final = df_merged.copy()
        df_final['D&A dedicado'] = 0

    df_final['Custo FP Veiculo'] = df_final['Custo Rateado'] + df_final['D&A dedicado']

    # ============================================================
    # ETAPA 3: COMPARAR resultado simulado vs Real vs BE existente
    # ============================================================
    print("\n" + "=" * 80)
    print("ETAPA 3: COMPARACAO FINAL (por Veiculo+Periodo)")
    print("=" * 80)

    chaves = ['Veículo', 'Período']

    # Simulado (acabamos de calcular)
    sim_agg = df_final.groupby(chaves)['Custo FP Veiculo'].sum().reset_index()
    sim_agg = sim_agg.rename(columns={'Custo FP Veiculo': 'Simulado'})

    # Real
    real_jf2 = real_veic[real_veic['Período'].isin(meses)]
    real_agg = real_jf2.groupby(chaves)['Custo FP Veiculo'].sum().reset_index()
    real_agg = real_agg.rename(columns={'Custo FP Veiculo': 'Real'})

    # BE existente
    be_jf2 = be_veic[be_veic['Período'].isin(meses)]
    be_agg = be_jf2.groupby(chaves)['Custo FP Veiculo'].sum().reset_index()
    be_agg = be_agg.rename(columns={'Custo FP Veiculo': 'BE_Existente'})

    comp = pd.merge(real_agg, sim_agg, on=chaves, how='outer')
    comp = pd.merge(comp, be_agg, on=chaves, how='outer')
    for c in ['Real', 'Simulado', 'BE_Existente']:
        comp[c] = comp[c].fillna(0)
    comp['Diff_Sim_Real'] = comp['Simulado'] - comp['Real']
    comp['Diff_BE_Real'] = comp['BE_Existente'] - comp['Real']

    print(comp.to_string(index=False))

    print(f"\nTotais:")
    print(f"  Real: {comp['Real'].sum():,.2f}")
    print(f"  Simulado: {comp['Simulado'].sum():,.2f}")
    print(f"  BE Existente: {comp['BE_Existente'].sum():,.2f}")
    print(f"  Diff Simulado-Real: {comp['Diff_Sim_Real'].sum():,.2f}")
    print(f"  Diff BE-Real: {comp['Diff_BE_Real'].sum():,.2f}")

    # ============================================================
    # ETAPA 4: Detalhar divergencias por Veiculo+Oficina+Periodo
    # ============================================================
    chaves2 = ['Veículo', 'Oficina', 'Período']
    sim_det = df_final.groupby(chaves2)['Custo FP Veiculo'].sum().reset_index().rename(columns={'Custo FP Veiculo': 'Simulado'})
    real_det = real_jf2.groupby(chaves2)['Custo FP Veiculo'].sum().reset_index().rename(columns={'Custo FP Veiculo': 'Real'})
    comp_det = pd.merge(real_det, sim_det, on=chaves2, how='outer').fillna(0)
    comp_det['Diff'] = comp_det['Simulado'] - comp_det['Real']
    divergentes = comp_det[comp_det['Diff'].abs() > 1].sort_values('Diff', key=abs, ascending=False)

    print(f"\n--- DETALHAMENTO Veiculo+Oficina+Periodo (|diff| > R$ 1) ---")
    print(f"Total divergencias: {len(divergentes)}")
    if len(divergentes) > 0:
        print(divergentes.head(30).to_string(index=False))

    # ============================================================
    # ETAPA 5: Verificar se Custo FP entrada == resultado por veiculo
    # ============================================================
    print("\n" + "=" * 80)
    print("ETAPA 5: FECHAMENTO")
    print("=" * 80)
    custo_fp_entrada = df_entrada['Custo FP'].sum()
    custo_fp_veic = df_final['Custo FP Veiculo'].sum()
    print(f"  Custo FP entrada (FC Jan/Fev): {custo_fp_entrada:,.2f}")
    print(f"  Custo FP Veiculo resultado: {custo_fp_veic:,.2f}")
    print(f"  Diferenca: {custo_fp_veic - custo_fp_entrada:,.2f}")

    return comp, comp_det


if __name__ == '__main__':
    diagnostico_completo()
