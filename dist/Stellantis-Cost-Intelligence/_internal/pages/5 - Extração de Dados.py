import streamlit as st
import pandas as pd
import os
import shutil
import subprocess
from datetime import datetime
from versionamento import obter_versao_atual
import sys
import re
import unicodedata
from tc_core.utils.portabilidade import get_base_path, get_data_root

# Adicionar o diretório raiz ao path para importar os módulos de processamento
_ROOT = str(get_base_path())
_DATA_ROOT = str(get_data_root())
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    from processamento_dados import processar_completo as processar_dados_reais_completo
    from processamento_dados_BUD import processar_completo_bud as processar_dados_budget_completo
except ImportError as e:
    st.error(f"❌ Erro ao importar módulos de processamento: {e}")
    st.stop()

# Importar módulo de alertas (opcional — não bloqueia a página se não existir)
_ALERTAS_DISPONIVEL = False
try:
    from alertas.alert_engine import run_daily_check
    _ALERTAS_DISPONIVEL = True
except ImportError:
    pass


def _em_execucao_empacotada():
    return getattr(sys, '_frozen', False) or getattr(sys, 'frozen', False)


def _selecionar_arquivo_excel_desktop(nome_arquivo: str) -> str | None:
    """Abre seletor nativo do Windows para contornar falhas do uploader no desktop."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        caminho = filedialog.askopenfilename(
            title=f"Selecionar {nome_arquivo}",
            filetypes=[("Excel", "*.xlsx")],
        )
        root.destroy()
        return caminho or None
    except Exception as e:
        st.error(f"❌ Não foi possível abrir o seletor de arquivos: {e}")
        return None


def _executar_alertas_pos_extracao():
    """Executa verificação de alertas após extração de dados e exibe resultado."""
    if not _ALERTAS_DISPONIVEL:
        return
    try:
        with st.spinner("🔔 Verificando alertas do SCI..."):
            alertas = run_daily_check()
        if alertas:
            n = len(alertas)
            enviados = sum(
                1 for a in alertas
                if a.get("notificacoes_enviadas", {}).get("email")
                or a.get("notificacoes_enviadas", {}).get("teams")
            )
            st.info(
                f"🔔 **Central de Alertas:** {n} alerta(s) processado(s), "
                f"{enviados} notificação(ões) enviada(s)."
            )
        else:
            st.success("🔔 Central de Alertas: nenhum desvio identificado.")
    except Exception as e:
        st.warning(f"⚠️ Alertas não puderam ser verificados: {e}")

# Função para obter data e hora de atualização dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da última atualização dos arquivos de dados"""
    try:
        arquivos_dados = [
            os.path.join(_DATA_ROOT, "TC_Ext", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join(_DATA_ROOT, "TC_Ext", "historico_consolidado", "df_vol_historico.parquet"),
            os.path.join(_DATA_ROOT, "TC_Ext", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
        ]
        
        data_atualizacao = None
        for arquivo in arquivos_dados:
            if os.path.exists(arquivo):
                try:
                    data_modificacao = os.path.getmtime(arquivo)
                    if data_modificacao and data_modificacao > 0:
                        if data_atualizacao is None or data_modificacao > data_atualizacao:
                            data_atualizacao = data_modificacao
                except (OSError, ValueError):
                    continue
        
        if data_atualizacao and data_atualizacao > 0:
            try:
                dt = datetime.fromtimestamp(data_atualizacao)
                meses = {
                    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
                    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
                    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                }
                return f"{dt.day:02d} de {meses[dt.month]} de {dt.year} às {dt.hour:02d}:{dt.minute:02d}"
            except (ValueError, OSError):
                return None
        return None
    except Exception:
        return None

# CSS para reduzir fonte das configurações da sidebar
st.markdown("""
    <style>
        .block-container {
            padding-top: 4rem !important;
            padding-bottom: 0.25rem !important;
        }
        div[data-testid="stVerticalBlock"] {
            gap: 0.35rem !important;
        }
        hr {
            margin: 0.18rem 0 !important;
            opacity: 0.2 !important;
        }
        /* Reduzir fonte do header da sidebar */
        .css-1d391kg h3 {
            font-size: 0.9rem !important;
        }
        /* Reduzir fonte dos radio buttons da sidebar */
        .css-1d391kg div[data-testid="stRadio"] label {
            font-size: 0.75rem !important;
        }
        .css-1d391kg div[data-testid="stRadio"] label p {
            font-size: 0.75rem !important;
        }
        /* Reduzir fonte do number input da sidebar */
        .css-1d391kg div[data-testid="stNumberInput"] label {
            font-size: 0.75rem !important;
        }
        .css-1d391kg div[data-testid="stNumberInput"] label p {
            font-size: 0.75rem !important;
        }
        /* Reduzir fonte do info box da sidebar */
        .css-1d391kg .stAlert {
            font-size: 0.75rem !important;
        }
        .css-1d391kg .stAlert p {
            font-size: 0.75rem !important;
        }
        .css-1d391kg .stAlert strong {
            font-size: 0.8rem !important;
        }
    </style>
""", unsafe_allow_html=True)

# Função para obter mês atual em português
def obter_mes_atual():
    """Retorna o mês atual em português"""
    meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    agora = datetime.now()
    return meses[agora.month]

# Cabeçalho compacto com data de atualização
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
# Obter data de atualização (a função já está definida acima)
data_atualizacao = obter_data_atualizacao_dados()

# Montar textos do cabeçalho
texto_esquerda = f"📚 Stellantis Cost Intelligence (SCI) | Versão {versao_atual} | {mes_atual} {ano_atual} | Desenvolvido por Hudson Cardin e Lauro Paiva"
texto_direita = f"📅 Dados atualizados em: {data_atualizacao}" if data_atualizacao else ""

st.markdown(f"""
<div style='display: flex; justify-content: space-between; align-items: center; color: #fff; padding: 8px 10px; font-size: 0.85rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-bottom: 1px solid #5a4fcf; margin-bottom: 10px;'>
    <div style='flex: 1;'>{texto_esquerda}</div>
    <div style='flex: 0 0 auto; margin-left: 20px;'>{texto_direita}</div>
</div>
""", unsafe_allow_html=True)


# Título
st.title("📥 Extração e Processamento de Dados")
st.markdown("---")

# Seleção de configurações na página principal
col_config1, col_config2 = st.columns(2)

with col_config1:
    tipo_extracao = st.radio(
        "📊 Selecione o tipo de extração:",
        ["📊 Dados REAIS (tc_ext/notebooks/dados.ipynb)", "💰 Dados BUDGET (tc_ext/notebooks/dados_BUD.ipynb)", "🔄 Ambos"],
        horizontal=True
    )

with col_config2:
    ano_padrao = datetime.now().year
    ano_selecionado = st.number_input(
        "📅 Ano para processar:",
        min_value=2020,
        max_value=2100,
        value=ano_padrao,
        step=1
    )

st.markdown("---")

# Sidebar - Informações
st.sidebar.header("ℹ️ Informações")
st.sidebar.info("""
**📋 Instruções:**
1. Selecione o tipo de extração
2. Informe o ano
3. Verifique os arquivos necessários
4. Execute o processamento

**🔄 Sincronização:**
Os módulos Python são convertidos dos notebooks `.ipynb` mantendo toda a lógica original.
Use o botão na página principal para verificar se estão atualizados.
""")

# ==========================================
# FUNÇÕES DE VALIDAÇÃO
# ==========================================

MESES_PT = [
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
]


def _validar_abas_excel(caminho: str, abas_obrigatorias: list[str], contexto: str) -> tuple[bool, list[str]]:
    msgs: list[str] = []
    try:
        xl = pd.ExcelFile(caminho)
        abas = xl.sheet_names
    except Exception as e:
        return False, [f"❌ Não foi possível abrir o Excel ({contexto}): {caminho}. Erro: {e}"]

    faltando = [a for a in abas_obrigatorias if a not in abas]
    if faltando:
        msgs.append(f"❌ Abas faltando em {contexto}: {faltando}")
        msgs.append(f"   Abas disponíveis: {abas}")
        return False, msgs

    msgs.append(f"✅ Abas OK em {contexto}: {abas_obrigatorias}")
    return True, msgs


def _encontrar_arquivo(ano: int, nome_arquivo: str, incluir_bud: bool = False) -> str | None:
    candidatos = [
        os.path.join(_DATA_ROOT, 'TC_Ext', str(ano), nome_arquivo),
        os.path.join('.', nome_arquivo),
    ]
    if incluir_bud:
        candidatos.insert(1, os.path.join(_DATA_ROOT, 'TC_Ext', str(ano), 'BUD', nome_arquivo))
    for c in candidatos:
        if os.path.exists(c):
            return c
    return None


def _extrair_colunas_rateio_like(caminho: str, sheet_name: str) -> tuple[list[str], list[str]]:
    """Tenta reproduzir a leitura do rateio (header em células) e retorna (colunas, colunas_mes)."""
    df_raw = pd.read_excel(caminho, sheet_name=sheet_name, header=None)

    # Igual ao processamento: pula primeira linha, pega a próxima como header
    df = df_raw.iloc[1:].reset_index(drop=True)
    if df.empty:
        return [], []
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df = df.loc[:, df.notna().any(axis=0)]
    df = df.dropna(axis=1, how='all')
    colunas = [str(c) for c in df.columns if pd.notna(c)]

    colunas_meses = []
    for c in colunas:
        c_lower = str(c).lower().strip()
        # robusto a variações/encoding (ex.: "mar�o") usando prefixos
        c_sem_acento = ''.join(
            ch for ch in unicodedata.normalize('NFKD', c_lower)
            if not unicodedata.combining(ch)
        )
        c_norm = re.sub(r'[^a-z0-9]', '', c_sem_acento)
        pref = c_norm[:3]
        if pref in {'jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'}:
            colunas_meses.append(c)
    return colunas, colunas_meses


def _normalizar_nome_coluna_debug(v: object) -> str:
    s = str(v).lower().strip()
    s = ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))
    return re.sub(r'[^a-z0-9]', '', s)


def _ler_volume_para_validacao(caminho: str, sheet_name: str) -> tuple[pd.DataFrame | None, str | None]:
    """Tenta ler a aba de Volume em múltiplos headers (layout antigo e novo)."""
    headers = [50, 0, 1, 2]
    last_err = None
    for h in headers:
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_name, header=h, nrows=5)
            return df, f"header={h}"
        except Exception as e:
            last_err = e
            continue
    return None, str(last_err) if last_err else None


def _validar_pre_extracao_reais(ano: int) -> tuple[bool, list[str]]:
    msgs: list[str] = []
    ok = True

    caminho_sapiens = _encontrar_arquivo(ano, 'Dados SAPIENS.xlsx', incluir_bud=False)
    caminho_rateio = _encontrar_arquivo(ano, 'Reporting fluxo anexo.xlsx', incluir_bud=False)
    if not caminho_sapiens or not caminho_rateio:
        return False, [
            "❌ Arquivos obrigatórios não encontrados para REAIS.",
            f"   Dados SAPIENS.xlsx: {caminho_sapiens}",
            f"   Reporting fluxo anexo.xlsx: {caminho_rateio}",
        ]

    ok_abas_rateio, m = _validar_abas_excel(caminho_rateio, ['Sapiens', 'Rateio', 'Volume'], 'Reporting fluxo anexo.xlsx')
    msgs.extend(m)
    ok &= ok_abas_rateio

    ok_abas_sapiens, m = _validar_abas_excel(caminho_sapiens, ['Base conso'], 'Dados SAPIENS.xlsx')
    msgs.extend(m)
    ok &= ok_abas_sapiens

    if ok_abas_rateio:
        # Sapiens
        try:
            df = pd.read_excel(caminho_rateio, sheet_name='Sapiens', header=1, nrows=5)
            cols = set([str(c) for c in df.columns])
            obrig = {'Valor', 'QTD', 'Oficina', 'Período', 'Account', 'USI'}
            if not obrig.issubset(cols):
                ok = False
                msgs.append(f"❌ Aba 'Sapiens': colunas faltando (mínimo esperado): {sorted(list(obrig - cols))}")
            else:
                msgs.append("✅ Aba 'Sapiens': colunas mínimas OK")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'Sapiens' (header=1): {e}")

        # Rateio
        try:
            colunas, colunas_meses = _extrair_colunas_rateio_like(caminho_rateio, 'Rateio')
            norm = {re.sub(r'\s+', '', c.lower()): c for c in colunas}
            tem_oficina = 'oficina' in norm
            tem_veiculo = ('veículo' in norm) or ('veiculo' in norm)
            if not tem_oficina or not tem_veiculo:
                ok = False
                falt = []
                if not tem_oficina:
                    falt.append('Oficina')
                if not tem_veiculo:
                    falt.append('Veículo/Veiculo')
                msgs.append(f"❌ Aba 'Rateio': colunas faltando: {falt}")
            if len(colunas_meses) == 0:
                ok = False
                msgs.append("❌ Aba 'Rateio': não encontrei colunas de meses (Janeiro..Dezembro)")
            else:
                msgs.append(f"✅ Aba 'Rateio': meses detectados: {len(colunas_meses)}")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Rateio': {e}")

        # Volume
        try:
            dfv, info = _ler_volume_para_validacao(caminho_rateio, 'Volume')
            if dfv is None:
                ok = False
                msgs.append(f"❌ Falha ao ler aba 'Volume' (tentativas header=50/0/1/2): {info}")
            else:
                colunas_norm = [_normalizar_nome_coluna_debug(c) for c in dfv.columns]
                pref_cols = [c[:3] for c in colunas_norm if c]
                meses = [p for p in pref_cols if p in {'jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'}]
                if 'oficina' not in colunas_norm:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume': coluna 'Oficina' não encontrada ({info})")
                # Regra esperada: Volume REAIS deve conter a dimensão Veículo
                if 'veiculo' not in colunas_norm:
                    ok = False
                    cols_preview = ', '.join([str(c) for c in dfv.columns[:30]])
                    msgs.append(f"❌ Aba 'Volume': coluna 'Veículo' não encontrada ({info}). Colunas (parcial): {cols_preview}")
                if len(meses) == 0:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume': não encontrei colunas de meses ({info})")
                else:
                    msgs.append(f"✅ Aba 'Volume': meses detectados: {len(set(meses))} ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume': {e}")

    return ok, msgs


def _validar_pre_extracao_budget(ano: int) -> tuple[bool, list[str]]:
    msgs: list[str] = []
    ok = True

    # Padronização (TC Ext): arquivos de entrada ficam em dados/TC_Ext/{ano}/ (mesma fonte para REAIS e BUDGET)
    caminho_sapiens = _encontrar_arquivo(ano, 'Dados SAPIENS.xlsx', incluir_bud=False)
    caminho_rateio = _encontrar_arquivo(ano, 'Reporting fluxo anexo.xlsx', incluir_bud=False)
    if not caminho_sapiens or not caminho_rateio:
        return False, [
            "❌ Arquivos obrigatórios não encontrados para BUDGET.",
            f"   Dados SAPIENS.xlsx: {caminho_sapiens}",
            f"   Reporting fluxo anexo.xlsx: {caminho_rateio}",
        ]

    ok_abas_rateio, m = _validar_abas_excel(caminho_rateio, ['Voz de custo BDG', 'Rateio BDG', 'Volume BDG'], 'Reporting fluxo anexo.xlsx')
    msgs.extend(m)
    ok &= ok_abas_rateio

    ok_abas_sapiens, m = _validar_abas_excel(caminho_sapiens, ['Base conso'], 'Dados SAPIENS.xlsx')
    msgs.extend(m)
    ok &= ok_abas_sapiens

    if ok_abas_rateio:
        try:
            df = pd.read_excel(caminho_rateio, sheet_name='Voz de custo BDG', nrows=5)
            cols = set([str(c) for c in df.columns])
            obrig = {'Oficina', 'Account'}
            if not obrig.issubset(cols):
                ok = False
                msgs.append(f"❌ Aba 'Voz de custo BDG': colunas faltando (mínimo esperado): {sorted(list(obrig - cols))}")
            else:
                msgs.append("✅ Aba 'Voz de custo BDG': colunas mínimas OK")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao ler aba 'Voz de custo BDG': {e}")

        try:
            colunas, colunas_meses = _extrair_colunas_rateio_like(caminho_rateio, 'Rateio BDG')
            norm = {re.sub(r'\s+', '', c.lower()): c for c in colunas}
            tem_oficina = 'oficina' in norm
            tem_veiculo = ('veículo' in norm) or ('veiculo' in norm)
            if not tem_oficina or not tem_veiculo:
                ok = False
                falt = []
                if not tem_oficina:
                    falt.append('Oficina')
                if not tem_veiculo:
                    falt.append('Veículo/Veiculo')
                msgs.append(f"❌ Aba 'Rateio BDG': colunas faltando: {falt}")
            if len(colunas_meses) == 0:
                ok = False
                msgs.append("❌ Aba 'Rateio BDG': não encontrei colunas de meses (Janeiro..Dezembro)")
            else:
                msgs.append(f"✅ Aba 'Rateio BDG': meses detectados: {len(colunas_meses)}")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Rateio BDG': {e}")

        try:
            dfv, info = _ler_volume_para_validacao(caminho_rateio, 'Volume BDG')
            if dfv is None:
                ok = False
                msgs.append(f"❌ Falha ao ler aba 'Volume BDG' (tentativas header=50/0/1/2): {info}")
            else:
                colunas_norm = [_normalizar_nome_coluna_debug(c) for c in dfv.columns]
                pref_cols = [c[:3] for c in colunas_norm if c]
                meses = [p for p in pref_cols if p in {'jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'}]
                if 'oficina' not in colunas_norm:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': coluna 'Oficina' não encontrada ({info})")
                # Governança: Volume BDG deve conter Veículo
                if 'veiculo' not in colunas_norm:
                    ok = False
                    cols_preview = ', '.join([str(c) for c in dfv.columns[:30]])
                    msgs.append(f"❌ Aba 'Volume BDG': coluna 'Veículo' não encontrada ({info}). Colunas (parcial): {cols_preview}")
                if len(meses) == 0:
                    ok = False
                    msgs.append(f"❌ Aba 'Volume BDG': não encontrei colunas de meses ({info})")
                else:
                    msgs.append(f"✅ Aba 'Volume BDG': meses detectados: {len(set(meses))} ({info})")
        except Exception as e:
            ok = False
            msgs.append(f"❌ Falha ao validar aba 'Volume BDG': {e}")

    return ok, msgs

def verificar_arquivos_reais(ano):
    """Verifica arquivos necessários para dados REAIS"""
    pasta_ano = os.path.join(_DATA_ROOT, 'TC_Ext', str(ano))
    arquivos_necessarios = {
        'Dados SAPIENS.xlsx': 'Base de dados SAPIENS',
        'Reporting fluxo anexo.xlsx': 'Dados de rateio/volume e Sapiens'
    }
    
    arquivos_ok = []
    arquivos_faltando = []
    
    for arquivo, descricao in arquivos_necessarios.items():
        caminho_ano = os.path.join(pasta_ano, arquivo)
        caminho_raiz = os.path.join('.', arquivo)
        
        if os.path.exists(caminho_ano):
            arquivos_ok.append((arquivo, caminho_ano, 'pasta_ano'))
        elif os.path.exists(caminho_raiz):
            arquivos_ok.append((arquivo, caminho_raiz, 'raiz'))
        else:
            arquivos_faltando.append((arquivo, descricao))
    
    return arquivos_ok, arquivos_faltando

def verificar_arquivos_budget(ano):
    """Verifica arquivos necessários para dados BUDGET"""
    pasta_ano = os.path.join(_DATA_ROOT, 'TC_Ext', str(ano))
    arquivos_necessarios = {
        'Dados SAPIENS.xlsx': 'Base de dados SAPIENS',
        'Reporting fluxo anexo.xlsx': 'Dados de rateio/volume'
    }
    
    arquivos_ok = []
    arquivos_faltando = []
    
    for arquivo, descricao in arquivos_necessarios.items():
        caminho_ano = os.path.join(pasta_ano, arquivo)
        caminho_raiz = os.path.join('.', arquivo)
        
        if os.path.exists(caminho_ano):
            arquivos_ok.append((arquivo, caminho_ano, 'pasta_ano'))
        elif os.path.exists(caminho_raiz):
            arquivos_ok.append((arquivo, caminho_raiz, 'raiz'))
        else:
            arquivos_faltando.append((arquivo, descricao))
    
    return arquivos_ok, arquivos_faltando

# ==========================================
# INTERFACE PRINCIPAL
# ==========================================

# Verificação de sincronização (dentro da página)
st.markdown("---")
col_sync1, col_sync2, col_sync3 = st.columns([2, 1, 1])
with col_sync1:
    st.markdown("### 🔄 Verificação de Sincronização")
    st.caption("Verifica se os módulos Python estão atualizados com os notebooks `.ipynb`")
with col_sync2:
    st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento
    verificar_sync = st.button("🔄 Verificar Sincronização", use_container_width=True, type="secondary")
with col_sync3:
    st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento

if verificar_sync:
    try:
        with st.spinner("🔄 Verificando sincronização..."):
            resultado = subprocess.run(
                [sys.executable, "sincronizar_notebooks.py"],
                capture_output=True,
                text=True,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
        
        with st.expander("📊 Relatório de Sincronização", expanded=True):
            st.code(resultado.stdout, language="text")
        
        if resultado.returncode == 0 and "sincronizados" in resultado.stdout:
            st.success("✅ Todos os módulos estão sincronizados com os notebooks!")
        else:
            st.warning("⚠️ Verifique o relatório acima para detalhes")
    except Exception as e:
        st.error(f"❌ Erro ao verificar sincronização: {e}")
        st.exception(e)

st.markdown("---")

# Tabs para organização
tab1, tab2, tab3 = st.tabs(["📋 Validação de Arquivos", "⚙️ Executar Processamento", "📊 Status e Logs"])

# TAB 1: Validação de Arquivos
with tab1:
    st.header("📋 Validação de Arquivos Necessários")
    
    # ==========================================
    # SEÇÃO DE UPLOAD DE ARQUIVOS
    # ==========================================
    st.markdown("### 📤 Upload de Arquivos")
    st.info("""
    **💡 Dica:** Se os arquivos não estiverem na pasta `dados/{ano_selecionado}/` ou na raiz do projeto,
    você pode fazer upload diretamente aqui. Os arquivos serão salvos automaticamente na pasta do ano.
    """)

    def _salvar_upload_unificado(
        *,
        label: str,
        nome_arquivo: str,
        key_uploader: str,
        help_text: str,
    ) -> None:
        """Renderiza 1 uploader e salva em um único local por ano.

        Padrão do projeto: os Excels de entrada (REAIS + BUDGET) ficam em:
        - dados/TC_Ext/{ano}/<arquivo>

        (Os outputs de BUDGET continuam indo para dados/TC_Ext/{ano}/BUD/ como antes.)
        """

        pasta_ano = os.path.join(_DATA_ROOT, 'TC_Ext', str(ano_selecionado))
        destino = os.path.join(pasta_ano, nome_arquivo)

        if os.path.exists(destino):
            st.warning(f"⚠️ Já existe: `{destino}`")
        else:
            st.caption(f"📁 Destino: `{destino}`")

        precisa_confirmar = os.path.exists(destino)
        confirmar = True
        if precisa_confirmar:
            confirmar = st.checkbox(
                f"Confirmar sobrescrita de `{nome_arquivo}`",
                value=False,
                key=f"{key_uploader}_confirm_overwrite",
            )

        if _em_execucao_empacotada():
            st.caption(
                "No app desktop, use o seletor nativo do Windows para evitar "
                "falhas do upload embutido."
            )
            if st.button(
                f"📂 Selecionar e salvar {nome_arquivo}",
                key=f"{key_uploader}_btn_save_desktop",
                use_container_width=False,
                type="primary",
                disabled=precisa_confirmar and not confirmar,
            ):
                origem = _selecionar_arquivo_excel_desktop(nome_arquivo)
                if origem:
                    os.makedirs(pasta_ano, exist_ok=True)
                    shutil.copy2(origem, destino)
                    st.success(f"✅ Arquivo salvo em: `{destino}`")
                    st.caption(f"Arquivo selecionado: `{origem}`")
                    st.rerun()
            return

        arquivo_upload = st.file_uploader(
            label,
            type=["xlsx"],
            key=key_uploader,
            help=help_text,
        )

        if arquivo_upload is None:
            return

        if st.button(
            f"💾 Salvar {nome_arquivo}",
            key=f"{key_uploader}_btn_save",
            use_container_width=False,
            type="primary",
            disabled=precisa_confirmar and not confirmar,
        ):
            os.makedirs(pasta_ano, exist_ok=True)
            with open(destino, "wb") as f:
                f.write(arquivo_upload.getbuffer())
            st.success(f"✅ Arquivo salvo em: `{destino}`")
            st.rerun()

    st.markdown("#### 📄 Arquivos (usados por REAIS e/ou BUDGET)")
    st.caption(
        "Os processamentos de REAIS e BUDGET usam os mesmos arquivos de entrada. "
        "Padrão: manter os Excels em `dados/TC_Ext/{ano}/`."
    )

    _salvar_upload_unificado(
        label="📄 Upload: Dados SAPIENS.xlsx",
        nome_arquivo="Dados SAPIENS.xlsx",
        key_uploader="upload_sapiens_unificado",
        help_text="Arquivo 'Dados SAPIENS.xlsx' (pode ser usado em REAIS e/ou BUDGET)",
    )

    _salvar_upload_unificado(
        label="📄 Upload: Reporting fluxo anexo.xlsx",
        nome_arquivo="Reporting fluxo anexo.xlsx",
        key_uploader="upload_rateio_unificado",
        help_text="Arquivo 'Reporting fluxo anexo.xlsx' (contém abas para REAIS e/ou BUDGET)",
    )
    


# TAB 2: Executar Processamento
with tab2:
    st.header("⚙️ Executar Processamento")
    
    st.info("""
    **⚠️ Importante:**
    - Certifique-se de que todos os arquivos necessários estão presentes
    - O processamento pode levar alguns minutos
    - Não feche a página durante a execução
    """)
    
    st.markdown("### 🔎 Pré-validação (recomendado)")
    colv1, colv2 = st.columns([1, 3])
    with colv1:
        btn_prevalidar = st.button(
            "🔎 Pré-validar estrutura dos Excel",
            use_container_width=True,
            type="secondary",
        )
    with colv2:
        st.caption(
            "Checa abas/colunas esperadas e aponta problemas antes de rodar a extração. "
            "Não executa o processamento nem grava parquets."
        )

    if btn_prevalidar:
        relatorio: list[str] = []
        ok_total = True

        if tipo_extracao in ["📊 Dados REAIS (tc_ext/notebooks/dados.ipynb)", "🔄 Ambos"]:
            ok_reais, msgs = _validar_pre_extracao_reais(int(ano_selecionado))
            ok_total &= ok_reais
            relatorio.append("📊 REAIS")
            relatorio.extend(msgs)

        if tipo_extracao in ["💰 Dados BUDGET (tc_ext/notebooks/dados_BUD.ipynb)", "🔄 Ambos"]:
            ok_bud, msgs = _validar_pre_extracao_budget(int(ano_selecionado))
            ok_total &= ok_bud
            relatorio.append("💰 BUDGET")
            relatorio.extend(msgs)

        with st.expander("📋 Relatório de Pré-validação", expanded=True):
            st.code("\n".join(relatorio), language="text")

        if ok_total:
            st.success("✅ Pré-validação OK. Pode executar a extração.")
        else:
            st.error("❌ Pré-validação falhou. Corrija os itens acima antes de executar.")

    st.markdown("---")

    col1, col2, col3 = st.columns(3)
    
    executar_reais = False
    executar_budget = False
    executar_ambos = False
    
    with col1:
        if tipo_extracao in ["📊 Dados REAIS (tc_ext/notebooks/dados.ipynb)", "🔄 Ambos"]:
            executar_reais = st.button(
                "🚀 Executar dados.ipynb (tc_ext/notebooks)",
                type="primary",
                use_container_width=True
            )
    
    with col2:
        if tipo_extracao in ["💰 Dados BUDGET (tc_ext/notebooks/dados_BUD.ipynb)", "🔄 Ambos"]:
            executar_budget = st.button(
                "🚀 Executar dados_BUD.ipynb (tc_ext/notebooks)",
                type="primary",
                use_container_width=True
            )
    
    with col3:
        if tipo_extracao == "🔄 Ambos":
            executar_ambos = st.button(
                "🚀 Executar Ambos",
                type="primary",
                use_container_width=True
            )
    
    # Container para logs
    log_container = st.container()
    
    # Executar processamentos
    executar_alertas_ao_final = False

    if executar_reais or (executar_ambos and tipo_extracao == "🔄 Ambos"):
        with log_container:
            st.subheader("📊 Processando Dados REAIS...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_messages = st.empty()
            
            mensagens_log = []
            
            def callback_progresso(mensagem):
                mensagens_log.append(mensagem)
                log_messages.text("\n".join(mensagens_log[-10:]))  # Mostrar últimas 10 mensagens
            
            try:
                with st.spinner("🔄 Processando dados REAIS..."):
                    resultado = processar_dados_reais_completo(
                        ano=ano_selecionado,
                        continuar_sem_arquivos=False,
                        progress_callback=callback_progresso
                    )
                    
                    progress_bar.progress(100)
                    status_text.success("✅ Processamento de dados REAIS concluído com sucesso!")
                    st.json(resultado)
                    executar_alertas_ao_final = True
            except Exception as e:
                progress_bar.progress(0)
                status_text.error(f"❌ Erro durante processamento: {str(e)}")
                st.exception(e)
    
    if executar_budget or (executar_ambos and tipo_extracao == "🔄 Ambos"):
        with log_container:
            st.subheader("💰 Processando Dados BUDGET...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_messages = st.empty()
            
            mensagens_log = []
            
            def callback_progresso(mensagem):
                mensagens_log.append(mensagem)
                log_messages.text("\n".join(mensagens_log[-10:]))
            
            try:
                with st.spinner("🔄 Processando dados BUDGET..."):
                    resultado = processar_dados_budget_completo(
                        ano=ano_selecionado,
                        continuar_sem_arquivos=False,
                        progress_callback=callback_progresso
                    )
                    
                    progress_bar.progress(100)
                    status_text.success("✅ Processamento de dados BUDGET concluído com sucesso!")
                    st.json(resultado)
                    executar_alertas_ao_final = True
            except Exception as e:
                progress_bar.progress(0)
                status_text.error(f"❌ Erro durante processamento: {str(e)}")
                st.exception(e)

    if executar_alertas_ao_final:
        _executar_alertas_pos_extracao()

# TAB 3: Status e Logs
with tab3:
    st.header("📊 Status e Logs")
    
    st.subheader("📁 Estrutura de Pastas")
    
    pasta_ext_ano = os.path.join(_DATA_ROOT, 'TC_Ext', str(ano_selecionado))
    if os.path.exists(pasta_ext_ano):
        st.success(f"✅ Pasta `dados/TC_Ext/{ano_selecionado}/` existe")
        
        # Listar arquivos na pasta do ano
        arquivos_ano = os.listdir(pasta_ext_ano)
        if arquivos_ano:
            st.markdown("**Arquivos na pasta do ano:**")
            for arquivo in arquivos_ano:
                caminho_completo = os.path.join(pasta_ext_ano, arquivo)
                if os.path.isfile(caminho_completo):
                    tamanho = os.path.getsize(caminho_completo) / (1024 * 1024)  # MB
                    data_mod = datetime.fromtimestamp(os.path.getmtime(caminho_completo))
                    st.text(f"  📄 {arquivo} ({tamanho:.2f} MB) - {data_mod.strftime('%d/%m/%Y %H:%M')}")
    else:
        st.warning(f"⚠️ Pasta `dados/TC_Ext/{ano_selecionado}/` não existe ainda")
    
    st.markdown("---")
    
    st.subheader("📚 Histórico Consolidado")
    
    pasta_hist = os.path.join(_DATA_ROOT, 'TC_Ext', 'historico_consolidado')
    if os.path.exists(pasta_hist):
        st.success("✅ Pasta `dados/TC_Ext/historico_consolidado/` existe")
        
        # Verificar arquivos principais
        arquivos_historico = [
            'df_final_historico.parquet',
            'df_vol_historico.parquet'
        ]
        
        for arquivo in arquivos_historico:
            caminho = os.path.join(pasta_hist, arquivo)
            if os.path.exists(caminho):
                tamanho = os.path.getsize(caminho) / (1024 * 1024)  # MB
                data_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                st.success(f"  ✅ {arquivo} ({tamanho:.2f} MB) - {data_mod.strftime('%d/%m/%Y %H:%M')}")
            else:
                st.warning(f"  ⚠️ {arquivo} não encontrado")
        
        # Verificar histórico BUD
        pasta_hist_bud = os.path.join(pasta_hist, 'BUD')
        if os.path.exists(pasta_hist_bud):
            st.markdown("**Histórico BUD:**")
            arquivos_historico_bud = [
                'df_final_historico_BUD.parquet',
                'df_vol_historico_BUD.parquet'
            ]
            
            for arquivo in arquivos_historico_bud:
                caminho = os.path.join(pasta_hist_bud, arquivo)
                if os.path.exists(caminho):
                    tamanho = os.path.getsize(caminho) / (1024 * 1024)  # MB
                    data_mod = datetime.fromtimestamp(os.path.getmtime(caminho))
                    st.success(f"  ✅ {arquivo} ({tamanho:.2f} MB) - {data_mod.strftime('%d/%m/%Y %H:%M')}")
                else:
                    st.warning(f"  ⚠️ {arquivo} não encontrado")
    else:
        st.warning("⚠️ Pasta `dados/TC_Ext/historico_consolidado/` não existe ainda")

# Rodapé
st.markdown("---")
mes_atual = datetime.now().strftime("%B")
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
meses = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}
mes_atual_nome = meses[datetime.now().month]

st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Stellantis Cost Intelligence (SCI) | Versão {versao_atual} | {mes_atual_nome} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin e Lauro Paiva</small>
</div>
""", unsafe_allow_html=True)

