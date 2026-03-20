"""
TC Veículos — Extração e Processamento de Dados
Replica o layout e funcionalidades do TC Ext (3 tabs + radio).
Upload com proteção contra sobrescrita, pré-validação, barra de progresso,
log ao vivo, consolidação histórica multi-ano e status de parquets.
"""

import streamlit as st
import pandas as pd
import os
import shutil
import sys
import re
import json
import time
import unicodedata
from datetime import datetime

from tc_core.utils.portabilidade import get_base_path, get_data_root, get_workspace_upload_root, is_cloud, probe_write_access
from tc_core.databricks_jobs import (
    get_databricks_run_output,
    get_tc_pipeline_run_status,
    submit_tc_pipeline_run,
    submit_tc_prevalidation_run,
    upload_file_to_dbfs,
    upload_file_to_workspace,
)
from tc_principal.ui_components import injetar_css_global, render_header

_PIPELINE_RUN_KEY = 'tc_pipeline_cloud_run'
_EXCEL_CANDIDATOS = (
    'Reporting veículos.xlsx',
    'Reporting veiculos.xlsx',
)

# Caminho DBFS para upload do Excel (API DBFS funciona sem limite de tamanho)
_DBFS_DATA_ROOT = "dbfs:/sci_data"

# ── Caminho raiz do projeto ──
_ROOT = str(get_base_path())
_DATA_ROOT = str(get_data_root())
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    from processamento_dados_veiculos_BUD import processar_veiculos_budget
except ImportError:
    processar_veiculos_budget = None

try:
    from processamento_dados_veiculos import processar_veiculos_real, executar_conferencias
except ImportError:
    processar_veiculos_real = None
    executar_conferencias = None

try:
    from alertas.alert_engine import run_daily_check
    _ALERTAS_DISPONIVEL = True
except ImportError:
    run_daily_check = None
    _ALERTAS_DISPONIVEL = False

# ════════════════════════════════════════════
# CONSTANTES
# ════════════════════════════════════════════

PASTA_TC = os.path.join(_DATA_ROOT, 'TC_Principal')
RATEIOS_PATH = os.path.join(_ROOT, 'rateios_manuais.json')
CONFIG_FONTES_PATH = os.path.join(_ROOT, 'config_fontes.json')

PARQUETS_BUDGET = [
    'df_principal_BUD.parquet',
    'df_vol_veiculos_BUD.parquet',
    'df_vol_veiculos_actual.parquet',
    'df_tempo_veiculos_BUD.parquet',
    'df_dea_dedicado_BUD.parquet',
    'df_volume_fa_BUD.parquet',
    'df_veiculos_fp_sem_da_BUD.parquet',
    'df_veiculos_percentual_rateio_BUD.parquet',
    'df_veiculos_custo_rateado_BUD.parquet',
    'df_veiculos_custo_fp_BUD.parquet',
    'df_veiculos_cpu_BUD.parquet',
]

PARQUETS_REAL = [
    'df_principal.parquet',
    'df_volume_fa.parquet',
    'df_tempo_veiculos.parquet',
    'df_vol_veiculos.parquet',
    'df_dea_dedicado.parquet',
    'df_veiculos_fp_sem_da.parquet',
    'df_veiculos_percentual_rateio.parquet',
    'df_veiculos_custo_rateado.parquet',
    'df_veiculos_custo_fp.parquet',
    'df_veiculos_cpu.parquet',
    'df_comparativo_real_budget.parquet',
]


def _atualizar_status_run(run_id: int, *, force: bool = False) -> dict:
    """Busca e cacheia o status do pipeline cloud no session_state."""
    key = _PIPELINE_RUN_KEY
    cached = st.session_state.get(key, {})
    is_terminal = cached.get('is_terminal', False)
    if not force and cached.get('run_id') == run_id and is_terminal:
        return cached
    try:
        status = get_tc_pipeline_run_status(run_id=run_id)
        st.session_state[key] = status
        return status
    except Exception as exc:
        cached['erro_status'] = str(exc)
        return cached


def _renderizar_painel_status_cloud(run_id: int, key_suffix: str = '') -> None:
    """Exibe painel de status do pipeline cloud com polling manual."""
    status = _atualizar_status_run(run_id)

    life = status.get('life_cycle_state', 'DESCONHECIDO')
    result = status.get('result_state') or ''
    is_terminal = status.get('is_terminal', False)
    erro = status.get('erro_status', '')

    # ── Banner de estado ──
    if erro:
        st.warning(f"⚠️ Não foi possível consultar o status: {erro}")
    elif not is_terminal:
        st.info(f"⏳ **Pipeline em execução** — Estado: `{life}`")
    elif result == 'SUCCESS':
        st.success("✅ **Processamento concluído com sucesso!** Acesse a Home para ver os dados.")
    else:
        msg = status.get('state_message') or result or 'Falha desconhecida'
        st.error(f"❌ **Pipeline encerrado com erro** — {msg}")

    # ── Métricas resumidas ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Run ID", str(run_id))
    c2.metric("Ciclo", life)
    c3.metric("Resultado", result or '—')
    dur_ms = (status.get('execution_duration') or 0) + (status.get('setup_duration') or 0)
    c4.metric("Duração", f"{dur_ms // 60000} min {(dur_ms % 60000) // 1000} s" if dur_ms else '—')

    # ── Tabela de tasks ──
    tasks = status.get('tasks') or []
    if tasks:
        import pandas as pd
        df_tasks = pd.DataFrame(tasks)
        colunas = [c for c in ['task_key', 'life_cycle_state', 'result_state', 'state_message'] if c in df_tasks.columns]
        st.dataframe(df_tasks[colunas], width="stretch", hide_index=True)

    # ── URL externa ──
    url = status.get('run_page_url') or ''
    col_btn, col_refresh = st.columns([3, 1])
    if url:
        col_btn.link_button("🔗 Abrir no Databricks", url, width="stretch")
    if col_refresh.button("🔄 Atualizar status", key=f'refresh_status_{key_suffix}', use_container_width=True):
        _atualizar_status_run(run_id, force=True)
        st.rerun()


def _em_execucao_empacotada() -> bool:
    return getattr(sys, 'frozen', False)


def _bloqueio_escrita_cloud(ano: int) -> str | None:
    if not is_cloud():
        return None

    # Verificar primeiro se DATA_ROOT sequer existe no container
    if not os.path.isdir(_DATA_ROOT):
        return (
            f"DATA_ROOT nao acessivel neste ambiente: {_DATA_ROOT}. "
            "Use os botoes abaixo para disparar o pipeline no cluster do Databricks."
        )

    destino_ano = os.path.join(_DATA_ROOT, 'TC_Principal', str(ano))
    write_ok, write_reason = probe_write_access(destino_ano)
    if write_ok:
        return None

    return (
        "Ambiente cloud com DATA_ROOT somente leitura. "
        "Use os botoes abaixo para disparar o pipeline no cluster do Databricks."
    )


def _disparar_pipeline_cloud(ano: int, tipo_extracao: str) -> dict:
    run_budget = tipo_extracao in ["💰 Dados BUDGET", "🔄 Ambos"]
    run_real = tipo_extracao in ["📊 Dados REAIS", "🔄 Ambos"]
    return submit_tc_pipeline_run(
        ano=ano,
        run_budget=run_budget,
        run_real=run_real,
    )


def _executar_prevalidacao_cloud(ano: int, tipo_extracao: str) -> tuple[bool, list[str]]:
    resposta = submit_tc_prevalidation_run(
        ano=ano,
        tipo_extracao=tipo_extracao,
        data_root=_DATA_ROOT,
    )
    run_id = resposta.get("run_id")
    if not run_id:
        return False, ["❌ Não foi possível iniciar a pré-validação no cluster."]

    inicio = time.time()
    while time.time() - inicio < 90:
        status = get_tc_pipeline_run_status(run_id=run_id)
        if status.get("is_terminal"):
            try:
                # Extrair task-level run_id (a API get-output exige
                # o run_id da task, não o job-level run_id)
                tasks = status.get("tasks") or []
                task_run_id = tasks[0].get("run_id") if tasks else run_id
                output = get_databricks_run_output(run_id=task_run_id)
                notebook_output = output.get("notebook_output") or {}
                result = notebook_output.get("result") or "{}"
                payload = json.loads(result)
                messages = payload.get("messages") or []
                if status.get("result_state") != "SUCCESS":
                    return False, messages or [
                        f"❌ Pré-validação falhou no cluster. Run ID: {run_id}"
                    ]
                return bool(payload.get("ok")), messages
            except Exception as exc:
                return False, [
                    f"❌ Pré-validação terminou, mas não foi possível ler o resultado: {exc}",
                    f"   Run ID: {run_id}",
                ]
        time.sleep(2)

    return False, [
        "❌ A pré-validação no cluster excedeu o tempo limite.",
        f"   Run ID: {run_id}",
    ]


# ════════════════════════════════════════════
# FUNÇÕES AUXILIARES
# ════════════════════════════════════════════

def _encontrar_arquivo(ano: int, nome_arquivo: str, incluir_bud: bool = False):
    # Gera variantes do nome para lidar com diferenças de encoding/formatação
    _VARIANTES_XLSX = {
        "Reporting veículos.xlsx": ("Reporting veículos.xlsx", "Reporting veiculos.xlsx", "Reporting_veiculos.xlsx"),
    }
    nomes = _VARIANTES_XLSX.get(nome_arquivo, (nome_arquivo,))
    for nome in nomes:
        candidatos = [
            os.path.join(_DATA_ROOT, 'TC_Principal', str(ano), nome),
            os.path.join('.', nome),
        ]
        if incluir_bud:
            candidatos.insert(1, os.path.join(_DATA_ROOT, 'TC_Principal', str(ano), 'BUD', nome))
        for c in candidatos:
            if os.path.exists(c):
                return c
    return None


def _encontrar_excel(ano: int, incluir_bud: bool = False):
    for nome in _EXCEL_CANDIDATOS:
        caminho = _encontrar_arquivo(ano, nome, incluir_bud=incluir_bud)
        if caminho:
            return caminho
    return None


def _validar_abas_excel(caminho: str, abas_obrigatorias: list, contexto: str):
    msgs = []
    try:
        xl = pd.ExcelFile(caminho)
        abas = xl.sheet_names
    except Exception as e:
        return False, [f"❌ Não foi possível abrir o Excel ({contexto}): {e}"]

    faltando = [a for a in abas_obrigatorias if a not in abas]
    if faltando:
        msgs.append(f"❌ Abas faltando em {contexto}: {faltando}")
        msgs.append(f"   Abas disponíveis: {abas}")
        return False, msgs

    msgs.append(f"✅ Abas OK em {contexto}: {abas_obrigatorias}")
    return True, msgs


def _normalizar_col(v) -> str:
    s = str(v).lower().strip()
    s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
    return re.sub(r'[^a-z0-9]', '', s)


def _extrair_colunas_rateio(caminho: str, sheet_name: str):
    df_raw = pd.read_excel(caminho, sheet_name=sheet_name, header=None)
    df = df_raw.iloc[1:].reset_index(drop=True)
    if df.empty:
        return [], []
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.loc[:, df.notna().any(axis=0)]
    df = df.dropna(axis=1, how='all')
    colunas = [str(c) for c in df.columns if pd.notna(c)]
    pref_meses = {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}
    colunas_meses = [c for c in colunas if _normalizar_col(c)[:3] in pref_meses]
    return colunas, colunas_meses


def _ler_volume_para_validacao(caminho: str, sheet_name: str):
    """Tenta múltiplos valores de header, validando se as colunas fazem sentido.
    
    Retorna o primeiro DataFrame cujas colunas contenham meses ou 'Veículo'/'Oficina'
    (indicando que o header correto foi encontrado).
    """
    pref_meses = {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}

    def _avaliar_colunas(colunas):
        cn = [_normalizar_col(c) for c in colunas]
        pref = [c[:3] for c in cn if c]
        qtd_meses = sum(1 for p in pref if p in pref_meses)
        tem_dim = 'oficina' in cn or 'veiculo' in cn or 'veculo' in cn
        return tem_dim, qtd_meses

    primeiro_ok = None  # fallback: primeiro que leu sem erro

    # 1) Heurística: encontrar a melhor linha de header na própria planilha
    try:
        amostra = pd.read_excel(caminho, sheet_name=sheet_name, header=None, nrows=80)
        melhor_h = None
        melhor_score = (-1, -1)  # (tem_dim, qtd_meses)

        for i in range(len(amostra.index)):
            linha = amostra.iloc[i].tolist()
            tem_dim, qtd_meses = _avaliar_colunas(linha)
            score = (1 if tem_dim else 0, qtd_meses)
            if score > melhor_score:
                melhor_score = score
                melhor_h = i

        if melhor_h is not None and melhor_score > (0, 0):
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=melhor_h, nrows=5)
            return df, f"header={melhor_h}"
    except Exception:
        pass

    # 2) Fallback: tentativas conhecidas
    for h in [50, 1, 2, 0]:
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=h, nrows=5)
            tem_dim, qtd_meses = _avaliar_colunas(df.columns)
            if tem_dim or qtd_meses > 0:
                return df, f"header={h}"
            if primeiro_ok is None:
                primeiro_ok = (df, f"header={h}")
        except Exception:
            continue

    if primeiro_ok is not None:
        return primeiro_ok
    return None, None


# ════════════════════════════════════════════
# PRÉ-VALIDAÇÃO
# ════════════════════════════════════════════

def _validar_pre_extracao_budget(ano: int):
    """Pré-validação para Budget (abas do Reporting veículos.xlsx)."""
    msgs = []
    ok = True

    caminho = _encontrar_excel(ano)
    if not caminho:
        return False, [
            "❌ Excel não encontrado.",
            f"   Caminho esperado: {_DATA_ROOT}/TC_Principal/{ano}/",
            "   Nomes aceitos: Reporting veículos.xlsx ou Reporting veiculos.xlsx",
        ]

    abas = [
        'massa primária - BDG', 'massa - REDIS',
        'Volume e EST PdR - BDG', 'Volume BDG', 'Volume Actual',
        'EST veículos - BDG', 'massa - D&A dedicado',
    ]
    ok_abas, m = _validar_abas_excel(caminho, abas, 'Reporting veículos.xlsx')
    msgs.extend(m)
    ok &= ok_abas

    if ok_abas:
        # massa primária - BDG
        try:
            df = pd.read_excel(caminho, sheet_name='massa primária - BDG', nrows=5)
            cols = {str(c) for c in df.columns}
            obrig = {'Oficina', 'Account'}
            faltando = obrig - cols
            if faltando:
                ok = False
                msgs.append(f"❌ Aba 'massa primária - BDG': colunas faltando: {sorted(faltando)}")
            else:
                msgs.append("✅ Aba 'massa primária - BDG': colunas mínimas OK")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'massa primária - BDG': {e}")

        # massa - REDIS
        try:
            df = pd.read_excel(caminho, sheet_name='massa - REDIS', nrows=5)
            cols = {str(c) for c in df.columns}
            if 'Oficina' not in cols:
                ok = False
                msgs.append("❌ Aba 'massa - REDIS': coluna 'Oficina' não encontrada")
            else:
                msgs.append("✅ Aba 'massa - REDIS': colunas OK")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'massa - REDIS': {e}")

        # Volume BDG
        try:
            dfv, info = _ler_volume_para_validacao(caminho, 'Volume BDG')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume BDG'")
            else:
                cn = [_normalizar_col(c) for c in dfv.columns]
                pref = [c[:3] for c in cn if c]
                meses = [p for p in pref if p in {'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'}]
                # Volume BDG em Reporting veículos.xlsx pode não ter 'Oficina'
                # (só tem 'Veículo'). Aceitar qualquer uma das duas.
                tem_oficina = 'oficina' in cn
                tem_veiculo = 'veiculo' in cn or 'veculo' in cn
                # Em alguns layouts, a dimensão vem na 1ª coluna sem nome
                # (ex.: "Unnamed: 0"), e no processamento essa coluna é tratada
                # como 'Veículo'. Aceitar esse cenário na pré-validação.
                dim_generica = [
                    c for c in cn
                    if c and c[:3] not in {'jan', 'fev', 'mar', 'abr', 'mai', 'jun',
                                           'jul', 'ago', 'set', 'out', 'nov', 'dez'}
                    and c not in {'ano', 'total'}
                ]
                tem_dim_implicita = bool(dim_generica)
                if not tem_oficina and not tem_veiculo and not tem_dim_implicita:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': coluna 'Oficina' ou 'Veículo' não encontrada ({info})")
                if not meses:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': nenhum mês detectado ({info})")
                else:
                    dim_label = 'Oficina' if tem_oficina else (
                        'Veículo' if tem_veiculo else 'Dimensão (coluna sem nome)'
                    )
                    msgs.append(f"✅ Aba 'Volume BDG': {len(set(meses))} meses detectados, dimensão '{dim_label}' OK ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume BDG': {e}")

        # Volume Actual
        try:
            dfv, info = _ler_volume_para_validacao(caminho, 'Volume Actual')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume Actual'")
            else:
                msgs.append(f"✅ Aba 'Volume Actual': legível ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume Actual': {e}")

    return ok, msgs


def _validar_pre_extracao_real(ano: int):
    """Pré-validação para Real (Sapiens)."""
    msgs = []
    ok = True

    caminho = _encontrar_excel(ano)
    if not caminho:
        return False, [
            "❌ Excel não encontrado.",
            f"   Caminho esperado: {_DATA_ROOT}/TC_Principal/{ano}/",
            "   Nomes aceitos: Reporting veículos.xlsx ou Reporting veiculos.xlsx",
        ]

    abas = ['Sapiens', 'Volume e EST PdR - Actual', 'Volume Actual', 'EST veículos - Actual']
    ok_abas, m = _validar_abas_excel(caminho, abas, 'Reporting veículos.xlsx')
    msgs.extend(m)
    ok &= ok_abas

    if ok_abas:
        # Sapiens
        try:
            df = pd.read_excel(caminho, sheet_name='Sapiens', header=1, nrows=5)
            cols = {str(c) for c in df.columns}
            obrig = {'Oficina', 'Account'}
            faltando = obrig - cols
            if faltando:
                ok = False
                msgs.append(f"❌ Aba 'Sapiens': colunas faltando: {sorted(faltando)}")
            else:
                msgs.append(f"✅ Aba 'Sapiens': colunas mínimas OK ({len(df.columns)} colunas)")
            if 'Valor' not in cols:
                ok = False
                msgs.append("❌ Aba 'Sapiens': coluna 'Valor' não encontrada")
            else:
                msgs.append("✅ Aba 'Sapiens': coluna 'Valor' presente")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'Sapiens': {e}")

        # Volume e EST PdR - Actual
        try:
            dfv, info = _ler_volume_para_validacao(caminho, 'Volume e EST PdR - Actual')
            if dfv is None:
                ok = False
                msgs.append("❌ Falha ao ler aba 'Volume e EST PdR - Actual'")
            else:
                msgs.append(f"✅ Aba 'Volume e EST PdR - Actual': legível ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume e EST PdR - Actual': {e}")

        # EST veículos - Actual
        try:
            dfe = pd.read_excel(caminho, sheet_name='EST veículos - Actual', header=1, nrows=5)
            msgs.append(f"✅ Aba 'EST veículos - Actual': legível ({len(dfe.columns)} colunas)")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'EST veículos - Actual': {e}")

    # D&A Budget (pré-requisito)
    caminho_dea = os.path.join(PASTA_TC, str(ano), 'BUD', 'df_dea_dedicado_BUD.parquet')
    if os.path.exists(caminho_dea):
        msgs.append("✅ D&A Dedicado Budget encontrado")
    else:
        msgs.append("⚠️ D&A Dedicado Budget não encontrado — precisa processar Budget antes")

    return ok, msgs


# ════════════════════════════════════════════
# CONSOLIDAÇÃO HISTÓRICA
# ════════════════════════════════════════════

def _consolidar_historico_tc_principal():
    """Consolida parquets de todos os anos em histórico multi-ano.

    - Real: df_principal, df_vol_veiculos, df_veiculos_cpu
    - Budget: df_principal_BUD, df_vol_veiculos_BUD, df_veiculos_cpu_BUD
    """
    resultados = []

    # Descobrir anos disponíveis
    anos = []
    if os.path.exists(PASTA_TC):
        for item in os.listdir(PASTA_TC):
            if os.path.isdir(os.path.join(PASTA_TC, item)) and item.isdigit():
                anos.append(int(item))
    anos = sorted(anos)

    if not anos:
        return ["⚠️ Nenhum ano encontrado em dados/TC_Principal/"]

    pasta_hist = os.path.join(PASTA_TC, 'historico_consolidado')
    pasta_hist_bud = os.path.join(pasta_hist, 'BUD')
    os.makedirs(pasta_hist, exist_ok=True)
    os.makedirs(pasta_hist_bud, exist_ok=True)

    def _consolidar(mapa_arquivos: dict, pasta_destino: str, sufixo: str = ''):
        """Consolida uma lista de parquets de vários anos.

        mapa_arquivos: {nome_historico: (nome_fonte, subpasta)}
        """
        for nome_hist, (nome_fonte, subpasta) in mapa_arquivos.items():
            dfs = []
            for a in anos:
                if subpasta:
                    caminho = os.path.join(PASTA_TC, str(a), subpasta, nome_fonte)
                else:
                    caminho = os.path.join(PASTA_TC, str(a), nome_fonte)
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
                df_final.to_parquet(destino)
                resultados.append(f"✅ {nome_hist}: {len(dfs)} ano(s) → {len(df_final):,} linhas")
            else:
                resultados.append(f"⚠️ {nome_hist}: nenhum dado encontrado")

    # Real
    _consolidar({
        'df_principal_historico.parquet': ('df_principal.parquet', ''),
        'df_vol_historico.parquet': ('df_vol_veiculos.parquet', ''),
        'df_cpu_historico.parquet': ('df_veiculos_cpu.parquet', ''),
        'df_veiculos_custo_fp_historico.parquet': ('df_veiculos_custo_fp.parquet', ''),
    }, pasta_hist)

    # Budget
    _consolidar({
        'df_principal_historico_BUD.parquet': ('df_principal_BUD.parquet', 'BUD'),
        'df_vol_historico_BUD.parquet': ('df_vol_veiculos_BUD.parquet', 'BUD'),
        'df_cpu_historico_BUD.parquet': ('df_veiculos_cpu_BUD.parquet', 'BUD'),
        'df_veiculos_custo_fp_historico_BUD.parquet': ('df_veiculos_custo_fp_BUD.parquet', 'BUD'),
    }, pasta_hist_bud)

    return resultados


# ════════════════════════════════════════════
# RATEIOS MANUAIS
# ════════════════════════════════════════════

def _carregar_rateios():
    if os.path.exists(RATEIOS_PATH):
        with open(RATEIOS_PATH, 'r') as f:
            return json.load(f)
    return {"QY": 0.087526, "GS": 0.086982, "SM": 0.075452}


def _salvar_rateios(rateios: dict):
    with open(RATEIOS_PATH, 'w') as f:
        json.dump(rateios, f, indent=2)


# ════════════════════════════════════════════
# CONFIGURAÇÃO DE FONTES (SharePoint)
# ════════════════════════════════════════════

def _carregar_config_fontes() -> dict:
    if os.path.exists(CONFIG_FONTES_PATH):
        with open(CONFIG_FONTES_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"caminho_base_sharepoint": "", "caminhos_por_ano": {}}


def _salvar_config_fontes(config: dict):
    with open(CONFIG_FONTES_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def _normalizar_caminho_local(caminho: str) -> str:
    """Normaliza caminhos locais do Windows para validação consistente.

    IMPORTANTE: aplica NFC unicode para evitar falha de isdir() em caminhos
    com caracteres acentuados vindos de text_input do browser (que pode
    entregar NFD, incompatível com a API de arquivos do Windows).
    """
    c = unicodedata.normalize('NFC', caminho.strip())
    return os.path.normpath(os.path.expandvars(os.path.expanduser(c)))


_SP_DEFAULTS = [
    r"C:\Users\u235107\Stellantis\GEIB - GEIB\Partagei_2026\1 - SÍNTESE\2 - REPORTING CPPR\13 - SCI",
    r"C:\Users\u235107\Stellantis\GEIB - GEIB\Partagei_2026\1 - SÍNTESE\2 - REPORTING CPPR\13 - SCI\2026",
]


def _resolver_pasta_sharepoint(ano: int) -> str | None:
    """Retorna o caminho local da pasta SharePoint para o ano, ou None.

    Tenta, em ordem:
    1. Override específico por ano em config_fontes.json
    2. Caminho base do config (pasta-mãe ou já pasta do ano)
    3. Caminhos default fixos para o ambiente do u235107
    """
    if is_cloud():
        return None  # SharePoint local não existe em ambiente cloud
    config = _carregar_config_fontes()
    # 1. Override específico por ano
    pasta = config.get('caminhos_por_ano', {}).get(str(ano), '').strip()
    if pasta and not _eh_url_sharepoint(pasta):
        for variante in _gerar_variantes_unicode(pasta):
            if os.path.isdir(variante):
                return variante

    # 2. Caminho base configurado
    base = config.get('caminho_base_sharepoint', '').strip()
    if base and not _eh_url_sharepoint(base):
        candidatos = list({base, os.path.join(base, str(ano))})
        for candidato in candidatos:
            for variante in _gerar_variantes_unicode(candidato):
                if os.path.isdir(variante):
                    return variante

    # 3. Defaults fixos (fallback para quando nenhum config está salvo ou o
    #    browser entregou o caminho com encoding diferente)
    for default in _SP_DEFAULTS:
        # tentar primeiro subpasta do ano, depois a própria pasta
        for candidato in (os.path.join(default, str(ano)), default):
            for variante in _gerar_variantes_unicode(candidato):
                if os.path.isdir(variante):
                    return variante

    return None


def _gerar_variantes_unicode(caminho: str) -> list[str]:
    """Retorna variantes NFC e NFD do caminho normalizado.

    O Windows aceita NFC; garante que tentamos ambos para robustez contra
    text_input do browser que pode retornar NFD para caracteres acentuados.
    """
    nfc = _normalizar_caminho_local(caminho)
    try:
        nfd = os.path.normpath(
            os.path.expandvars(
                os.path.expanduser(
                    unicodedata.normalize('NFD', caminho.strip())
                )
            )
        )
    except Exception:
        nfd = nfc
    return [nfc] if nfc == nfd else [nfc, nfd]


def _caminho_sharepoint_esperado(ano: int) -> str | None:
    """Retorna o caminho que será tentado para o ano informado."""
    config = _carregar_config_fontes()
    override = config.get('caminhos_por_ano', {}).get(str(ano), '').strip()
    if override and not _eh_url_sharepoint(override):
        return _normalizar_caminho_local(override)
    base = config.get('caminho_base_sharepoint', '').strip()
    if base and not _eh_url_sharepoint(base):
        base_norm = _normalizar_caminho_local(base)
        if os.path.basename(base_norm.rstrip('\\/')) == str(ano):
            return base_norm
        return os.path.join(base_norm, str(ano))
    # fallback: defaults do ambiente
    for default in _SP_DEFAULTS:
        candidato = default if os.path.basename(default) == str(ano) else os.path.join(default, str(ano))
        return _normalizar_caminho_local(candidato)
    return None


def _eh_url_sharepoint(texto: str) -> bool:
    """Detecta se o texto é uma URL do SharePoint em vez de caminho local."""
    t = texto.strip().lower()
    return t.startswith(('http://', 'https://')) or 'sharepoint.com' in t


def _detectar_onedrive_local() -> str | None:
    """Tenta detectar a pasta do OneDrive/SharePoint sincronizada localmente."""
    # Variáveis de ambiente
    for var in ('OneDriveCommercial', 'OneDrive'):
        val = os.environ.get(var, '').strip()
        if val and os.path.isdir(val):
            return val
    # Pastas comuns no perfil do usuário
    home = os.path.expanduser('~')
    for nome in os.listdir(home):
        caminho = os.path.join(home, nome)
        if os.path.isdir(caminho) and 'onedrive' in nome.lower():
            return caminho
    return None


def _buscar_excel_sharepoint(ano: int) -> tuple[bool, str]:
    """Tenta copiar o Excel da pasta SharePoint para dados/TC_Principal/{ano}/.

    Retorna (sucesso, mensagem).
    """
    if is_cloud():
        return False, (
            "Busca do SharePoint não disponível em ambiente cloud.\n"
            "Use o **upload de arquivo** ou **dispare o pipeline** pelo Databricks."
        )
    pasta_sp = _resolver_pasta_sharepoint(ano)
    if not pasta_sp:
        config = _carregar_config_fontes()
        base = config.get('caminho_base_sharepoint', '').strip()
        if not base:
            return False, "Caminho do SharePoint não configurado. Configure na barra lateral."
        if _eh_url_sharepoint(base):
            onedrive = _detectar_onedrive_local()
            msg = (
                "❌ Você informou uma **URL do SharePoint** em vez de um caminho local.\n\n"
                "O sistema precisa do caminho da pasta sincronizada no seu computador.\n\n"
                "**Como encontrar o caminho correto:**\n"
                "1. Abra o SharePoint no navegador\n"
                "2. Clique em **Sincronizar** (botão no topo)\n"
                "3. O OneDrive criará uma pasta local (ex: `C:\\Users\\...\\Stellantis\\GEIB\\...`)\n"
                "4. Navegue até a pasta que contém as subpastas por ano (2025, 2026...)\n"
                "5. Copie o caminho da barra de endereços do Explorador de Arquivos\n"
            )
            if onedrive:
                msg += f"\n📁 OneDrive detectado em: `{onedrive}`"
            return False, msg
        caminho_esperado = _caminho_sharepoint_esperado(ano) or base
        return False, f"Pasta do SharePoint não encontrada: {caminho_esperado}"

    # Procurar o arquivo na pasta SharePoint
    caminho_origem = None
    for nome in _EXCEL_CANDIDATOS:
        candidato = os.path.join(pasta_sp, nome)
        if os.path.exists(candidato):
            caminho_origem = candidato
            break

    if not caminho_origem:
        return False, (
            f"Arquivo não encontrado na pasta SharePoint:\n"
            f"   Pasta: {pasta_sp}\n"
            f"   Nomes buscados: {', '.join(_EXCEL_CANDIDATOS)}"
        )

    # Copiar para destino
    pasta_destino = os.path.join(_DATA_ROOT, 'TC_Principal', str(ano))
    os.makedirs(pasta_destino, exist_ok=True)
    destino = os.path.join(pasta_destino, 'Reporting veículos.xlsx')

    tam_origem = os.path.getsize(caminho_origem) / (1024 * 1024)
    dt_origem = datetime.fromtimestamp(os.path.getmtime(caminho_origem))

    shutil.copy2(caminho_origem, destino)

    return True, (
        f"✅ Arquivo copiado do SharePoint com sucesso!\n"
        f"   Origem: {caminho_origem}\n"
        f"   Destino: {destino}\n"
        f"   Tamanho: {tam_origem:.1f} MB | Data: {dt_origem:%d/%m/%Y %H:%M}"
    )



def _enviar_excel_ao_cloud(ano: int) -> tuple[bool, str]:
    """Envia o Excel local para o Databricks Workspace.

    Retorna (sucesso, mensagem).
    Em cloud, DBFS não é acessível ao container — envia só para Workspace.
    """
    # Procurar Excel local (nome original ou slugified)
    # No cloud, o arquivo salvo fica em /tmp/sci_data_cache/...
    pastas_busca = [os.path.join(_DATA_ROOT, 'TC_Principal', str(ano))]
    if is_cloud():
        pastas_busca.insert(0, os.path.join('/tmp/sci_data_cache', 'TC_Principal', str(ano)))
    _nomes = ('Reporting veículos.xlsx', 'Reporting_veiculos.xlsx', 'Reporting veiculos.xlsx')
    candidatos = [os.path.join(p, n) for p in pastas_busca for n in _nomes]
    local_path = next((p for p in candidatos if os.path.isfile(p)), None)
    if local_path is None:
        return False, (
            f"Arquivo local não encontrado:\n"
            f"   Pastas: {pastas_busca}\n\n"
            f"Primeiro faça o **upload** ou **busque do SharePoint**, "
            f"depois envie ao cloud."
        )

    # Destino: usar o nome original do arquivo encontrado (preservar acentos/espaços)
    nome_destino = os.path.basename(local_path)
    # Destino: Workspace Files (usado pelo processamento via get_data_root)
    ws_dest = f"{get_workspace_upload_root()}/TC_Principal/{ano}/{nome_destino}"

    msgs: list[str] = []

    # ── Upload Workspace Files (obrigatório) ──
    try:
        res_ws = upload_file_to_workspace(local_path, ws_dest, overwrite=True)
    except Exception as exc:
        res_ws = {"ok": False, "message": str(exc)}

    if res_ws.get("ok"):
        msgs.append(f"Workspace: `{ws_dest}` ✔")
    else:
        msgs.append(f"Workspace falhou: {res_ws.get('message', '?')}")

    ws_ok = res_ws.get("ok", False)

    # ── Upload DBFS (apenas local — em cloud pode falhar) ──
    if not is_cloud():
        dbfs_dest = f"{_DBFS_DATA_ROOT}/TC_Principal/{ano}/{nome_destino}"
        try:
            resultado = upload_file_to_dbfs(local_path, dbfs_dest, overwrite=True)
        except Exception as exc:
            resultado = {"ok": False, "message": str(exc)}
        if resultado.get("ok"):
            msgs.append(f"DBFS: `{dbfs_dest}` ✔")
        else:
            msgs.append(f"DBFS falhou: {resultado.get('message', '?')}")

    resumo = "\n   ".join(msgs)
    if ws_ok:
        return True, f"☁️ Excel enviado ao Databricks com sucesso!\n   {resumo}"
    return False, f"Falha no upload ao cloud:\n   {resumo}"


def _executar_alertas_pos_extracao() -> None:
    """Executa a Central de Alertas ao final do processamento."""
    if not _ALERTAS_DISPONIVEL or run_daily_check is None:
        return
    try:
        with st.spinner("🔔 Verificando alertas do SCI..."):
            alertas = run_daily_check()
        if alertas:
            enviados = sum(
                1 for alerta in alertas
                if alerta.get("notificacoes_enviadas", {}).get("email")
                or alerta.get("notificacoes_enviadas", {}).get("teams")
            )
            st.info(
                f"🔔 **Central de Alertas:** {len(alertas)} alerta(s) processado(s), "
                f"{enviados} notificação(ões) enviada(s)."
            )
        else:
            st.success("🔔 Central de Alertas: nenhum desvio identificado.")
    except Exception as exc:
        st.warning(f"⚠️ Alertas não puderam ser verificados: {exc}")


def _selecionar_arquivo_excel_desktop() -> str | None:
    """Usa o seletor nativo do Windows para upload no app desktop."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        caminho = filedialog.askopenfilename(
            title="Selecionar Reporting veículos.xlsx",
            filetypes=[("Excel", "*.xlsx")],
        )
        root.destroy()
        return caminho or None
    except Exception as exc:
        st.error(f"❌ Não foi possível abrir o seletor de arquivos: {exc}")
        return None


# ════════════════════════════════════════════
# RENDER PRINCIPAL
# ════════════════════════════════════════════

def render():
    injetar_css_global()
    render_header()

    st.title("📥 Extração e Processamento de Dados")
    st.caption("TC Veículos (Budget + Real)")
    st.markdown("---")

    # ── Controles na página principal ──
    col_cfg1, col_cfg2 = st.columns(2)

    with col_cfg1:
        tipo_extracao = st.radio(
            "📊 Selecione o tipo de extração:",
            ["📊 Dados REAIS", "💰 Dados BUDGET", "🔄 Ambos"],
            horizontal=True,
            key='ext_tipo',
        )

    with col_cfg2:
        ano_padrao = datetime.now().year
        ano_selecionado = st.number_input(
            "📅 Ano para processar:",
            min_value=2020,
            max_value=2100,
            value=ano_padrao,
            step=1,
            key='ext_ano',
        )

    bloqueio_escrita_cloud = _bloqueio_escrita_cloud(int(ano_selecionado))

    st.markdown("---")

    # ── Sidebar: Instruções + Rateios ──
    st.sidebar.header("ℹ️ Informações")
    st.sidebar.info("""
**📋 Instruções:**
1. Selecione o tipo de extração (Real / Budget / Ambos)
2. Informe o ano
3. Faça upload do Reporting veículos.xlsx se necessário
4. Execute a pré-validação
5. Inicie o processamento
""")

    # Rateios manuais na sidebar (sempre acessível)
    st.sidebar.markdown("---")
    st.sidebar.subheader("📐 Rateios Manuais")
    st.sidebar.caption("QY / GS / SM — usados no cálculo da taxa PDR.")
    rateios = _carregar_rateios()

    r_qy = st.sidebar.number_input("QY", value=rateios.get('QY', 0.087526),
                                    format="%.6f", step=0.000001, key='rat_qy')
    r_gs = st.sidebar.number_input("GS", value=rateios.get('GS', 0.086982),
                                    format="%.6f", step=0.000001, key='rat_gs')
    r_sm = st.sidebar.number_input("SM", value=rateios.get('SM', 0.075452),
                                    format="%.6f", step=0.000001, key='rat_sm')

    if st.sidebar.button("💾 Salvar Rateios", type="primary", use_container_width=True):
        _salvar_rateios({"QY": r_qy, "GS": r_gs, "SM": r_sm})
        st.sidebar.success("✅ Rateios salvos!")

    # Configuração de fontes (SharePoint / caminho local)
    st.sidebar.markdown("---")
    st.sidebar.subheader("📂 Fonte de Dados (SharePoint)")
    st.sidebar.caption(
        "Informe o **caminho local** da pasta sincronizada do SharePoint "
        "(via OneDrive/Teams). Não use links da web (https://...).\n\n"
        "Exemplo: `C:\\Users\\...\\Stellantis\\GEIB\\..\\SCI`"
    )
    _cfg_fontes = _carregar_config_fontes()
    # Mostrar o valor salvo; se vazio, mostrar o default para orientar o usuário
    _sp_base_salvo = _cfg_fontes.get('caminho_base_sharepoint', '')
    _sp_base_placeholder = _SP_DEFAULTS[0] if _SP_DEFAULTS else ''
    _sp_base = st.sidebar.text_input(
        "📁 Pasta base local (sincronizada)",
        value=_sp_base_salvo or _sp_base_placeholder,
        key='cfg_sp_base',
        help="Pasta-mãe sincronizada via OneDrive/Teams. Subpastas com o ano (ex: .../2025/, .../2026/) serão buscadas automaticamente.",
    )
    # Alerta se usuário digitou URL
    if _sp_base.strip() and _eh_url_sharepoint(_sp_base):
        st.sidebar.error(
            "⚠️ Isso é uma URL da web, não um caminho local!\n\n"
            "Clique em **Sincronizar** no SharePoint e use o caminho "
            "da pasta que o OneDrive criar no seu computador."
        )
    _sp_ano_salvo = _cfg_fontes.get('caminhos_por_ano', {}).get(str(ano_selecionado), '')
    _sp_ano_placeholder = _SP_DEFAULTS[1] if len(_SP_DEFAULTS) > 1 else ''
    _sp_ano_default = _sp_ano_salvo or (_SP_DEFAULTS[1] if len(_SP_DEFAULTS) > 1 else '')
    _sp_ano_override = st.sidebar.text_input(
        f"📁 Pasta específica para {ano_selecionado} (opcional)",
        value=_sp_ano_default,
        placeholder=_sp_ano_placeholder,
        key='cfg_sp_ano',
        help="Se preenchida, esta pasta será usada no lugar de {base}/{ano}/.",
    )
    if st.sidebar.button("💾 Salvar Config. Fontes", type="primary", use_container_width=True):
        # Só sobrescreve se o usuário digitou algo — nunca apaga com string vazia
        novo_base = _sp_base.strip()
        if novo_base:
            _cfg_fontes['caminho_base_sharepoint'] = novo_base
        novo_override = _sp_ano_override.strip()
        if novo_override:
            _cfg_fontes.setdefault('caminhos_por_ano', {})[str(ano_selecionado)] = novo_override
        elif str(ano_selecionado) in _cfg_fontes.get('caminhos_por_ano', {}):
            # Só remove se campo estava preenchido e foi apagado intencionalmente
            pass
        _salvar_config_fontes(_cfg_fontes)
        st.sidebar.success("✅ Configuração de fontes salva!")

    # ═══════════════════════════════════════
    #  TABS
    # ═══════════════════════════════════════

    tab1, tab2, tab3 = st.tabs([
        "📋 Validação de Arquivos",
        "⚙️ Executar Processamento",
        "📊 Status e Logs",
    ])

    # ─────────────────────────────────────
    #  TAB 1: Validação de Arquivos
    # ─────────────────────────────────────
    with tab1:
        st.header("📋 Validação de Arquivos Necessários")

        if bloqueio_escrita_cloud:
            st.warning(bloqueio_escrita_cloud)

        # ── Upload unificado ──
        st.markdown("### 📤 Upload de Arquivo")
        st.info(
            f"**💡 Dica:** O arquivo `Reporting veículos.xlsx` deve estar em "
            f"`dados/TC_Principal/{ano_selecionado}/`. Se necessário, faça upload abaixo."
        )

        pasta_ano = os.path.join(_DATA_ROOT, 'TC_Principal', str(ano_selecionado))
        # No cloud, salvar em /tmp (container não monta Workspace externo)
        if is_cloud():
            pasta_ano = os.path.join('/tmp/sci_data_cache', 'TC_Principal', str(ano_selecionado))
        destino = os.path.join(pasta_ano, "Reporting veículos.xlsx")

        # Se já existe, mostra info
        if os.path.exists(destino):
            tam = os.path.getsize(destino) / (1024 * 1024)
            dt_mod = datetime.fromtimestamp(os.path.getmtime(destino))
            st.warning(f"⚠️ Já existe: `{destino}` ({tam:.1f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
        else:
            st.caption(f"📁 Destino: `{destino}`")

        precisa_confirmar = os.path.exists(destino)
        confirmar = True
        if precisa_confirmar:
            confirmar = st.checkbox(
                "Confirmar sobrescrita do arquivo existente",
                value=False,
                key="upload_confirm_overwrite",
            )

        if _em_execucao_empacotada() and not is_cloud():
            st.info(
                "No app desktop, use o seletor nativo do Windows abaixo. "
                "Isso evita falhas do upload no navegador embutido."
            )
            if st.button(
                "📂 Selecionar e salvar Reporting veículos.xlsx",
                key="btn_upload_desktop_tc",
                type="primary",
                disabled=(precisa_confirmar and not confirmar) or bool(bloqueio_escrita_cloud),
            ):
                origem = _selecionar_arquivo_excel_desktop()
                if origem:
                    os.makedirs(pasta_ano, exist_ok=True)
                    shutil.copy2(origem, destino)
                    st.success(f"✅ Arquivo salvo em: `{destino}`")
                    st.caption(f"Arquivo selecionado: `{origem}`")
                    # Auto-enviar ao cloud
                    with st.spinner("Enviando ao Databricks Workspace..."):
                        ok_c, msg_c = _enviar_excel_ao_cloud(int(ano_selecionado))
                    if ok_c:
                        st.success(msg_c)
                    else:
                        st.warning(f"Salvo localmente, mas falha ao enviar ao cloud: {msg_c}")
        else:
            arquivo_upload = st.file_uploader(
                "📄 Upload: Reporting veículos.xlsx",
                type=["xlsx"],
                key="upload_reporting_tc",
                help="Arquivo principal contendo abas Budget e Real.",
            )

            if arquivo_upload is not None and st.button(
                "💾 Salvar Reporting veículos.xlsx",
                key="btn_salvar_upload",
                use_container_width=False,
                type="primary",
                disabled=(precisa_confirmar and not confirmar) or bool(bloqueio_escrita_cloud),
            ):
                os.makedirs(pasta_ano, exist_ok=True)
                with open(destino, "wb") as f:
                    f.write(arquivo_upload.getbuffer())
                st.success(f"✅ Arquivo salvo: `{destino}`")
                # Enviar ao cloud (Workspace + DBFS)
                with st.spinner("Enviando ao Databricks Workspace..."):
                    ok_c, msg_c = _enviar_excel_ao_cloud(int(ano_selecionado))
                if ok_c:
                    st.success(msg_c)
                else:
                    st.warning(f"Salvo localmente, mas falha ao enviar ao cloud: {msg_c}")

        st.divider()

        # ── Importar e Publicar no Databricks ──
        st.markdown("### 📂 Importar e Publicar no Databricks")

        _pasta_excel = os.path.join(_DATA_ROOT, 'TC_Principal', str(ano_selecionado))
        _excel_candidatos = [
            os.path.join(_pasta_excel, 'Reporting veículos.xlsx'),
            os.path.join(_pasta_excel, 'Reporting_veiculos.xlsx'),
            os.path.join(_pasta_excel, 'Reporting veiculos.xlsx'),
        ]
        _excel_local = next((p for p in _excel_candidatos if os.path.isfile(p)), None)
        _tem_excel = _excel_local is not None

        if _tem_excel:
            _tam = os.path.getsize(_excel_local) / (1024 * 1024)
            _dt = datetime.fromtimestamp(os.path.getmtime(_excel_local))
            st.caption(
                f"📄 Excel: `{os.path.basename(_excel_local)}` — "
                f"{_tam:.1f} MB | {_dt:%d/%m/%Y %H:%M}"
            )
        else:
            st.caption("⚠️ Nenhum Excel local encontrado.")

        if not is_cloud():
            _pasta_sp_resolvida = _resolver_pasta_sharepoint(int(ano_selecionado))
            if _pasta_sp_resolvida:
                st.caption(f"📁 SharePoint: `{_pasta_sp_resolvida}`")
            else:
                _cfg_f = _carregar_config_fontes()
                _base_f = _cfg_f.get('caminho_base_sharepoint', '').strip()
                if not _base_f:
                    st.caption("ℹ️ Pasta SharePoint não configurada — configure na barra lateral.")
                else:
                    _esperado = _caminho_sharepoint_esperado(int(ano_selecionado)) or _base_f
                    st.caption(f"⚠️ Pasta não encontrada: `{_esperado}`")
            st.caption(
                "Ao clicar, o sistema busca o Excel na pasta SharePoint (barra lateral), "
                "copia para a pasta local e envia ao Databricks."
            )
        else:
            st.caption(
                "Ao clicar, o Excel já salvo (upload acima) será publicado no Workspace Databricks."
            )

        if st.button(
            "📥 Importar e Publicar no Databricks",
            key="btn_importar_publicar_tc",
            type="primary",
            use_container_width=True,
        ):
            if not is_cloud():
                # LOCAL: buscar do SharePoint + enviar ao cloud
                with st.spinner("Buscando do SharePoint..."):
                    ok_sp, msg_sp = _buscar_excel_sharepoint(int(ano_selecionado))
                if ok_sp:
                    st.success(msg_sp)
                else:
                    st.warning(msg_sp)
                # Enviar ao cloud (re-detectar Excel após busca)
                _excel_atualizado = next(
                    (p for p in _excel_candidatos if os.path.isfile(p)), None,
                )
                if _excel_atualizado:
                    with st.spinner("Enviando ao Databricks Workspace..."):
                        ok_c, msg_c = _enviar_excel_ao_cloud(int(ano_selecionado))
                    if ok_c:
                        st.success(msg_c)
                    else:
                        st.error(msg_c)
                    st.rerun()
                else:
                    st.warning(
                        "⚠️ Nenhum Excel encontrado. Faça upload acima "
                        "ou configure o SharePoint na barra lateral."
                    )
            else:
                # CLOUD: publicar Excel existente
                _uploader = st.session_state.get("upload_reporting_tc")
                if _uploader is not None:
                    os.makedirs(pasta_ano, exist_ok=True)
                    with open(destino, "wb") as f:
                        f.write(_uploader.getbuffer())
                    st.info(f"📥 Arquivo do upload salvo em cache: `{destino}`")
                with st.spinner("Enviando ao Databricks Workspace..."):
                    ok_cloud, msg_cloud = _enviar_excel_ao_cloud(int(ano_selecionado))
                if ok_cloud:
                    st.success(msg_cloud)
                else:
                    st.error(msg_cloud)

        st.divider()

        # ── Pré-validação ──
        st.markdown("### 🔎 Pré-validação (recomendado)")
        col_v1, col_v2 = st.columns([1, 3])
        with col_v1:
            btn_prevalidar = st.button(
                "🔎 Pré-validar estrutura do Excel",
                use_container_width=True,
                type="secondary",
            )
        with col_v2:
            st.caption(
                "Checa abas e colunas esperadas antes de executar. "
                "Não grava parquets."
            )

        if btn_prevalidar:
            relatorio = []
            ok_total = True

            if bloqueio_escrita_cloud:
                with st.spinner("🔎 Validando Excel no cluster do Databricks..."):
                    ok_total, relatorio = _executar_prevalidacao_cloud(
                        int(ano_selecionado),
                        tipo_extracao,
                    )
            else:
                if tipo_extracao in ["📊 Dados REAIS", "🔄 Ambos"]:
                    ok_r, msgs = _validar_pre_extracao_real(int(ano_selecionado))
                    ok_total &= ok_r
                    relatorio.append("─── 📊 REAIS ───")
                    relatorio.extend(msgs)

                if tipo_extracao in ["💰 Dados BUDGET", "🔄 Ambos"]:
                    ok_b, msgs = _validar_pre_extracao_budget(int(ano_selecionado))
                    ok_total &= ok_b
                    relatorio.append("─── 💰 BUDGET ───")
                    relatorio.extend(msgs)

            with st.expander("📋 Relatório de Pré-validação", expanded=True):
                st.code("\n".join(relatorio), language="text")

            if ok_total:
                st.success("✅ Pré-validação OK — pode executar a extração.")
            else:
                st.error("❌ Corrija os itens acima antes de executar.")

    # ─────────────────────────────────────
    #  TAB 2: Executar Processamento
    # ─────────────────────────────────────
    with tab2:
        st.header("⚙️ Executar Processamento")
        st.info("""
**⚠️ Importante:**
- Certifique-se de que todos os arquivos necessários estão presentes
- O processamento pode levar alguns minutos
- Não feche a página durante a execução
        """)

        if bloqueio_escrita_cloud:
            st.info(
                "☁️ **Ambiente cloud** — Os botões abaixo disparam o processamento "
                "no cluster do Databricks e mostram o status aqui mesmo."
            )

        # Botões de execução
        col_b1, col_b2, col_b3 = st.columns(3)
        houve_sucesso_processamento = False

        # ── Busca automática do SharePoint antes de processar ──
        _deve_processar_local = not bloqueio_escrita_cloud

        executar_reais = False
        executar_budget = False
        executar_ambos = False

        with col_b1:
            if tipo_extracao in ["📊 Dados REAIS", "🔄 Ambos"]:
                executar_reais = st.button(
                    "🚀 Processar Real (Sapiens)",
                    type="primary",
                    use_container_width=True,
                )

        with col_b2:
            if tipo_extracao in ["💰 Dados BUDGET", "🔄 Ambos"]:
                executar_budget = st.button(
                    "🚀 Processar Budget",
                    type="primary",
                    use_container_width=True,
                )

        with col_b3:
            if tipo_extracao == "🔄 Ambos":
                executar_ambos = st.button(
                    "🚀 Executar Ambos",
                    type="primary",
                    use_container_width=True,
                )

        # Container de logs
        log_container = st.container()

        # ── Mostrar painel de status de run anterior (se existir) ──
        run_anterior = st.session_state.get(_PIPELINE_RUN_KEY, {})
        if bloqueio_escrita_cloud and run_anterior.get('run_id') and not (
            executar_reais or executar_budget or executar_ambos
        ):
            st.markdown("---")
            st.markdown("### 📡 Status do último processamento")
            _renderizar_painel_status_cloud(run_anterior['run_id'], key_suffix='tab2_prev')

        if bloqueio_escrita_cloud and (
            executar_reais or executar_budget or executar_ambos
        ):
            tipo_pipeline = tipo_extracao
            if executar_reais:
                tipo_pipeline = "📊 Dados REAIS"
            elif executar_budget:
                tipo_pipeline = "💰 Dados BUDGET"
            elif executar_ambos:
                tipo_pipeline = "🔄 Ambos"

            with log_container:
                st.subheader("☁️ Disparando pipeline no Databricks...")
                try:
                    resposta = _disparar_pipeline_cloud(
                        int(ano_selecionado),
                        tipo_pipeline,
                    )
                    run_id = resposta.get("run_id")
                    # Salvar no session_state para acompanhamento
                    st.session_state[_PIPELINE_RUN_KEY] = {
                        'run_id': run_id,
                        'ano': int(ano_selecionado),
                        'tipo': tipo_pipeline,
                        'run_page_url': resposta.get('run_page_url', ''),
                        'is_terminal': False,
                    }
                    st.success(f"✅ Pipeline iniciado — Run ID: **{run_id}**")
                    st.markdown("#### 📡 Acompanhamento em tempo real")
                    _renderizar_painel_status_cloud(run_id, key_suffix='tab2_new')
                except Exception as exc:
                    st.error(f"❌ Não foi possível disparar o pipeline: {exc}")

        # ── Busca automática do SharePoint quando usuário clica processar (somente local) ──
        if not is_cloud() and _deve_processar_local and (executar_reais or executar_budget or executar_ambos):
            ok_sp, msg_sp = _buscar_excel_sharepoint(int(ano_selecionado))
            if ok_sp:
                st.success(msg_sp)
            elif _resolver_pasta_sharepoint(int(ano_selecionado)):
                st.warning(msg_sp)

        # ── Processamento REAIS ──
        if (
            not bloqueio_escrita_cloud
            and (executar_reais or (executar_ambos and tipo_extracao == "🔄 Ambos"))
        ):
            with log_container:
                st.subheader("📊 Processando Dados REAIS...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                log_messages = st.empty()

                mensagens_log = []

                def callback_reais(mensagem):
                    mensagens_log.append(mensagem)
                    log_messages.text("\n".join(mensagens_log[-10:]))

                if processar_veiculos_real is None:
                    st.error("❌ Módulo `processamento_dados_veiculos` não encontrado.")
                else:
                    try:
                        with st.spinner("🔄 Processando dados REAIS..."):
                            resultado = processar_veiculos_real(
                                ano=int(ano_selecionado),
                                progress_callback=callback_reais,
                            )

                            progress_bar.progress(100)
                            status_text.success("✅ Processamento Real concluído!")
                            houve_sucesso_processamento = True

                            # Consolidar histórico
                            status_text_hist = st.empty()
                            status_text_hist.info("🔄 Consolidando histórico...")
                            hist_msgs = _consolidar_historico_tc_principal()
                            status_text_hist.success("✅ Histórico consolidado!")

                            # Limpar cache para forçar releitura dos parquets atualizados
                            st.cache_data.clear()

                            with st.expander("📁 Arquivos gerados", expanded=False):
                                if 'arquivos' in resultado:
                                    for nome, caminho_arq in resultado['arquivos'].items():
                                        st.write(f"  ✅ {nome}")
                                st.markdown("**Consolidação:**")
                                for msg in hist_msgs:
                                    st.write(msg)

                            # ══ Conferência Automática Real ══
                            if executar_conferencias is not None:
                                with st.expander("📋 Conferência Automática (Real × Excel)", expanded=True):
                                    try:
                                        df_conf = executar_conferencias(int(ano_selecionado), tipo='real')
                                        # Colorir status
                                        def _color_status(val):
                                            if '✅' in str(val):
                                                return 'background-color: #d4edda'
                                            elif '❌' in str(val):
                                                return 'background-color: #f8d7da'
                                            elif '⚠️' in str(val):
                                                return 'background-color: #fff3cd'
                                            return ''
                                        st.dataframe(
                                            df_conf.style.applymap(_color_status, subset=['Status']),
                                            width="stretch",
                                            hide_index=True,
                                        )
                                        n_ok = df_conf['Status'].str.contains('✅').sum()
                                        n_err = df_conf['Status'].str.contains('❌').sum()
                                        n_warn = df_conf['Status'].str.contains('⚠️').sum()
                                        st.caption(f"✅ {n_ok} OK | ⚠️ {n_warn} Atenção | ❌ {n_err} Divergências")
                                    except Exception as e_conf:
                                        st.warning(f"⚠️ Conferência não disponível: {e_conf}")

                    except Exception as e:
                        progress_bar.progress(0)
                        status_text.error(f"❌ Erro: {str(e)}")
                        st.exception(e)

        # ── Processamento BUDGET ──
        if (
            not bloqueio_escrita_cloud
            and (executar_budget or (executar_ambos and tipo_extracao == "🔄 Ambos"))
        ):
            with log_container:
                st.subheader("💰 Processando Dados BUDGET...")
                progress_bar_b = st.progress(0)
                status_text_b = st.empty()
                log_messages_b = st.empty()

                mensagens_log_b = []

                def callback_budget(mensagem):
                    mensagens_log_b.append(mensagem)
                    log_messages_b.text("\n".join(mensagens_log_b[-10:]))

                if processar_veiculos_budget is None:
                    st.error("❌ Módulo `processamento_dados_veiculos_BUD` não encontrado.")
                else:
                    try:
                        with st.spinner("🔄 Processando dados BUDGET..."):
                            resultado = processar_veiculos_budget(
                                ano=int(ano_selecionado),
                                progress_callback=callback_budget,
                            )

                            progress_bar_b.progress(100)
                            status_text_b.success("✅ Processamento Budget concluído!")
                            houve_sucesso_processamento = True

                            # Consolidar histórico
                            status_text_hist_b = st.empty()
                            status_text_hist_b.info("🔄 Consolidando histórico...")
                            hist_msgs = _consolidar_historico_tc_principal()
                            status_text_hist_b.success("✅ Histórico consolidado!")

                            # Limpar cache para forçar releitura dos parquets atualizados
                            st.cache_data.clear()

                            with st.expander("📁 Arquivos gerados", expanded=False):
                                if 'arquivos' in resultado:
                                    for nome, caminho_arq in resultado['arquivos'].items():
                                        st.write(f"  ✅ {nome}")
                                st.markdown("**Consolidação:**")
                                for msg in hist_msgs:
                                    st.write(msg)

                            # ══ Conferência Automática Budget ══
                            if executar_conferencias is not None:
                                with st.expander("📋 Conferência Automática (Budget × Excel)", expanded=True):
                                    try:
                                        df_conf_b = executar_conferencias(int(ano_selecionado), tipo='budget')
                                        def _color_status_b(val):
                                            if '✅' in str(val):
                                                return 'background-color: #d4edda'
                                            elif '❌' in str(val):
                                                return 'background-color: #f8d7da'
                                            elif '⚠️' in str(val):
                                                return 'background-color: #fff3cd'
                                            return ''
                                        st.dataframe(
                                            df_conf_b.style.applymap(_color_status_b, subset=['Status']),
                                            width="stretch",
                                            hide_index=True,
                                        )
                                        n_ok = df_conf_b['Status'].str.contains('✅').sum()
                                        n_err = df_conf_b['Status'].str.contains('❌').sum()
                                        n_warn = df_conf_b['Status'].str.contains('⚠️').sum()
                                        st.caption(f"✅ {n_ok} OK | ⚠️ {n_warn} Atenção | ❌ {n_err} Divergências")
                                    except Exception as e_conf:
                                        st.warning(f"⚠️ Conferência não disponível: {e_conf}")

                    except Exception as e:
                        progress_bar_b.progress(0)
                        status_text_b.error(f"❌ Erro: {str(e)}")
                        st.exception(e)

        if houve_sucesso_processamento:
            _executar_alertas_pos_extracao()

    # ─────────────────────────────────────
    #  TAB 3: Status e Logs
    # ─────────────────────────────────────
    with tab3:
        st.header("📊 Status e Logs")

        # ── Painel cloud: status do pipeline se houver run ativo ──
        if is_cloud():
            run_state = st.session_state.get(_PIPELINE_RUN_KEY, {})
            run_id_tab3 = run_state.get('run_id')
            if run_id_tab3:
                st.markdown("### 📡 Status do Pipeline Cloud")
                _renderizar_painel_status_cloud(run_id_tab3, key_suffix='tab3')
                st.divider()
            else:
                st.info(
                    "Nenhum pipeline disparado nesta sessão. "
                    "Execute o processamento na aba **⚙️ Executar Processamento**."
                )
                st.divider()

        # ── Budget Parquets ──
        st.markdown("### 💰 Parquets Budget")
        pasta_bud = os.path.join(PASTA_TC, str(ano_selecionado), 'BUD')

        for arq in PARQUETS_BUDGET:
            caminho = os.path.join(pasta_bud, arq)
            if os.path.exists(caminho):
                tam = os.path.getsize(caminho) / (1024 * 1024)
                dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                try:
                    df_tmp = pd.read_parquet(caminho)
                    linhas = len(df_tmp)
                    colunas = len(df_tmp.columns)
                    st.success(f"✅ `{arq}` — {tam:.2f} MB | {linhas:,} linhas × {colunas} cols | {dt_mod:%d/%m/%Y %H:%M}")
                except Exception:
                    st.success(f"✅ `{arq}` — {tam:.2f} MB | {dt_mod:%d/%m/%Y %H:%M}")
            else:
                st.warning(f"⚠️ `{arq}` não encontrado")

        st.divider()

        # ── Real Parquets ──
        st.markdown("### 📊 Parquets Real (Sapiens)")
        pasta_real = os.path.join(PASTA_TC, str(ano_selecionado))

        for arq in PARQUETS_REAL:
            caminho = os.path.join(pasta_real, arq)
            if os.path.exists(caminho):
                tam = os.path.getsize(caminho) / (1024 * 1024)
                dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                try:
                    df_tmp = pd.read_parquet(caminho)
                    linhas = len(df_tmp)
                    colunas = len(df_tmp.columns)
                    st.success(f"✅ `{arq}` — {tam:.2f} MB | {linhas:,} linhas × {colunas} cols | {dt_mod:%d/%m/%Y %H:%M}")
                except Exception:
                    st.success(f"✅ `{arq}` — {tam:.2f} MB | {dt_mod:%d/%m/%Y %H:%M}")
            else:
                st.warning(f"⚠️ `{arq}` não encontrado")

        st.divider()

        # ── Histórico Consolidado ──
        st.markdown("### 📚 Histórico Consolidado")

        pasta_hist = os.path.join(PASTA_TC, 'historico_consolidado')
        pasta_hist_bud = os.path.join(pasta_hist, 'BUD')

        hist_real = [
            'df_principal_historico.parquet',
            'df_vol_historico.parquet',
            'df_cpu_historico.parquet',
        ]
        hist_bud = [
            'df_principal_historico_BUD.parquet',
            'df_vol_historico_BUD.parquet',
            'df_cpu_historico_BUD.parquet',
        ]

        if os.path.exists(pasta_hist):
            st.markdown("**Real:**")
            for arq in hist_real:
                caminho = os.path.join(pasta_hist, arq)
                if os.path.exists(caminho):
                    tam = os.path.getsize(caminho) / (1024 * 1024)
                    dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                    st.success(f"  ✅ {arq} ({tam:.2f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
                else:
                    st.warning(f"  ⚠️ {arq} não encontrado")

            if os.path.exists(pasta_hist_bud):
                st.markdown("**Budget:**")
                for arq in hist_bud:
                    caminho = os.path.join(pasta_hist_bud, arq)
                    if os.path.exists(caminho):
                        tam = os.path.getsize(caminho) / (1024 * 1024)
                        dt_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                        st.success(f"  ✅ {arq} ({tam:.2f} MB) — {dt_mod:%d/%m/%Y %H:%M}")
                    else:
                        st.warning(f"  ⚠️ {arq} não encontrado")
            else:
                st.warning("⚠️ Pasta histórico Budget não existe ainda")
        else:
            st.warning("⚠️ Pasta `dados/TC_Principal/historico_consolidado/` não existe ainda")

        # Botão para forçar re-consolidação
        if st.button("🔄 Re-consolidar Histórico", type="secondary"):
            with st.spinner("Consolidando..."):
                msgs = _consolidar_historico_tc_principal()
            for m in msgs:
                st.write(m)
            st.success("✅ Consolidação concluída!")
            st.rerun()

        st.divider()

        # ── Árvore de pastas ──
        st.markdown("### 📁 Estrutura de Pastas")
        pasta_raiz_ano = os.path.join(_DATA_ROOT, str(ano_selecionado))
        pasta_tc_ano = os.path.join(PASTA_TC, str(ano_selecionado))

        for label, pasta in [
            (f"dados/{ano_selecionado}/", pasta_raiz_ano),
            (f"dados/TC_Principal/{ano_selecionado}/", pasta_tc_ano),
        ]:
            if os.path.exists(pasta):
                arquivos = []
                for root, dirs, files in os.walk(pasta):
                    for f in files:
                        fp = os.path.join(root, f)
                        rel = os.path.relpath(fp, pasta)
                        tam = os.path.getsize(fp) / (1024 * 1024)
                        arquivos.append(f"  📄 {rel} ({tam:.2f} MB)")
                if arquivos:
                    st.markdown(f"**`{label}`**")
                    st.code("\n".join(sorted(arquivos)), language="text")
                else:
                    st.info(f"`{label}` — pasta vazia.")
            else:
                st.caption(f"`{label}` não existe.")

    # ── Rodapé ──
    st.divider()
    st.caption(f"TC — Veículos | Extração | {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    render()

