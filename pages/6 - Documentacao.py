import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import base64
import sys
from datetime import datetime
from versionamento import obter_versao_atual

# Configuração da página
st.set_page_config(
    page_title="Documentação - Sistema TC",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

# Função para obter data e hora de atualização dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da última atualização dos arquivos de dados"""
    try:
        arquivos_dados = [
            os.path.join("dados", "TC_Ext", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join("dados", "TC_Ext", "historico_consolidado", "df_vol_historico.parquet"),
            os.path.join("dados", "TC_Ext", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
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
                return "Não disponível"
        return None
    except Exception:
        return None

# Cabeçalho compacto com data de atualização
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
data_atualizacao = obter_data_atualizacao_dados()

# Montar textos do cabeçalho
texto_esquerda = f"📚 Documentação Completa do Sistema TC | Versão {versao_atual} | {mes_atual} {ano_atual} | Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus"
texto_direita = f"📅 Dados atualizados em: {data_atualizacao}" if data_atualizacao else ""

st.markdown(f"""
<div style='display: flex; justify-content: space-between; align-items: center; color: #fff; padding: 8px 10px; font-size: 0.85rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-bottom: 1px solid #5a4fcf; margin-bottom: 10px;'>
    <div style='flex: 1;'>{texto_esquerda}</div>
    <div style='flex: 0 0 auto; margin-left: 20px;'>{texto_direita}</div>
</div>
""", unsafe_allow_html=True)


# CSS para melhorar visualização
st.markdown("""
    <style>
        h1 {
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 10px 20px;
            font-weight: 600;
        }
    </style>
""", unsafe_allow_html=True)

st.title("📚 Documentação Completa do Sistema TC")


def _ir_para_especificacao_tecnica() -> None:
    st.session_state["indice_documentacao"] = "🧾 Especificação Técnica"
    st.rerun()

# Função para detectar caminho base correto
def get_base_path():
    """Retorna o caminho base correto para LEITURA de dados"""
    import sys
    if hasattr(sys, '_MEIPASS'):
        # Rodando no executável PyInstaller - apontar para _internal
        return sys._MEIPASS
    else:
        # Rodando em desenvolvimento
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Funções para persistir dados da equipe
def salvar_dados_equipe(dados):
    """Salva os dados da equipe em arquivo JSON"""
    try:
        base_path = get_base_path()
        dados_path = os.path.join(base_path, 'dados_equipe.json')
        with open(dados_path, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar dados: {e}")
        return False

def carregar_dados_equipe():
    """Carrega os dados da equipe do arquivo JSON"""
    _estrutura_vazia = {
        'hudson': {
            'cargo': '', 'empresa': '', 'experiencia': '',
            'linkedin': '', 'foto': None,
            'papel_projeto': 'Full-Stack Developer',
            'descricao_papel': (
                'Desenvolvendo tanto a interface quanto a '
                'lógica e os cálculos do sistema'
            ),
        },
        'lauro': {
            'cargo': '', 'empresa': '', 'experiencia': '',
            'linkedin': '', 'foto': None,
            'papel_projeto': 'Full-Stack Developer',
            'descricao_papel': (
                'Desenvolvendo tanto a interface quanto a '
                'lógica e os cálculos do sistema'
            ),
        },
        'frederico': {
            'cargo': 'Manufacturing Finance Controller',
            'empresa': 'Stellantis',
            'experiencia': '',
            'linkedin': '', 'foto': None,
            'papel_projeto': 'Tech Advisor',
            'descricao_papel': (
                'Orientação técnica estratégica, validações '
                'e suporte de alto nível ao projeto'
            ),
        },
    }
    try:
        base_path = get_base_path()
        dados_path = os.path.join(base_path, 'dados_equipe.json')
        if os.path.exists(dados_path):
            with open(dados_path, 'r', encoding='utf-8') as f:
                dados = json.load(f)
            # Garantir que todos os membros e campos existam
            for chave, padrao in _estrutura_vazia.items():
                if chave not in dados:
                    dados[chave] = padrao
                else:
                    for campo, valor in padrao.items():
                        if campo not in dados[chave]:
                            dados[chave][campo] = valor
            return dados
    except Exception as e:
        st.warning(f"Aviso ao carregar dados: {e}")

    return _estrutura_vazia

def salvar_foto_base64(foto_bytes, nome_arquivo):
    """Converte foto para base64 para salvar no JSON"""
    try:
        return base64.b64encode(foto_bytes).decode('utf-8')
    except:
        return None

def carregar_foto_base64(foto_base64):
    """Converte base64 de volta para bytes"""
    try:
        if foto_base64:
            return base64.b64decode(foto_base64)
    except:
        pass
    return None

# Sidebar com índices
st.sidebar.markdown("## 📑 Índice")
st.sidebar.markdown("---")

# Seletor de módulo (TC Extendido ou TC Veículos)
modulo_doc = st.sidebar.radio(
    "Módulo:",
    ["📊 TC Extendido", "🚗 TC Veículos"],
    horizontal=True,
    key="modulo_documentacao"
)
st.sidebar.markdown("---")

# Criar índices no sidebar
indice_selecionado = st.sidebar.radio(
    "Selecione a seção:",
    [
        "👥 Equipe do Projeto",
        "📐 Regras e Cálculo",
        "🧮 Cálculo por Tabelas/Gráficos (Normal vs CPU)",
        "🏗️ Arquitetura e Estrutura",
        "🧾 Especificação Técnica",
        "📥 Guia de Extração de Dados",
        "🔮 Guia de Best Estimate",
        "📊 Apresentação Visual",
        "💬 Chatbot de Documentação",
    ],
    key="indice_documentacao"
)

st.markdown("---")

# ==========================================
# SEÇÃO 1: EQUIPE DO PROJETO
# ==========================================
if indice_selecionado == "👥 Equipe do Projeto":
    st.header("👥 Equipe do Projeto")
    
    st.markdown("""
    Esta seção apresenta os membros da equipe responsáveis pelo desenvolvimento
    e manutenção do **Sistema TC** — suas funções no projeto e perfis profissionais.
    """)

    # CSS para cards da equipe
    st.markdown("""
    <style>
        .team-badge-fullstack {
            display: inline-block;
            background: linear-gradient(135deg, #7C3AED, #6D28D9);
            color: white;
            padding: 4px 14px;
            border-radius: 20px;
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.3px;
            margin-bottom: 4px;
        }
        .team-badge-advisor {
            display: inline-block;
            background: linear-gradient(135deg, #2563EB, #1D4ED8);
            color: white;
            padding: 4px 14px;
            border-radius: 20px;
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.3px;
            margin-bottom: 4px;
        }
        .team-role-desc {
            font-size: 0.82rem;
            color: #9CA3AF;
            font-style: italic;
            margin-top: 2px;
            margin-bottom: 8px;
        }
        .team-photo-box {
            width: 180px;
            height: 200px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            margin: 0 auto 8px auto;
            background: transparent;
        }
        .team-photo-box img {
            max-width: 100%;
            max-height: 100%;
            object-fit: cover;
            border-radius: 10px;
        }
        .team-photo-placeholder {
            color: #6B7280;
            font-size: 3rem;
            line-height: 1;
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("---")

    dados_equipe = carregar_dados_equipe()

    # ── Definição dos membros ──
    membros = [
        {
            'key': 'hudson',
            'nome': 'Hudson Cardin',
            'icone': '🔧',
            'badge_class': 'team-badge-fullstack',
        },
        {
            'key': 'lauro',
            'nome': 'Lauro Paiva Junior',
            'icone': '📊',
            'badge_class': 'team-badge-fullstack',
        },
        {
            'key': 'frederico',
            'nome': 'Frederico Cesar de Jesus',
            'icone': '🧭',
            'badge_class': 'team-badge-advisor',
        },
    ]

    cols = st.columns(3)

    for col, membro in zip(cols, membros):
        k = membro['key']
        dados_m = dados_equipe.get(k, {})
        papel = dados_m.get('papel_projeto', '')
        desc_papel = dados_m.get('descricao_papel', '')

        with col:
            # ── Cabeçalho: nome + badge ──
            st.subheader(f"{membro['icone']} {membro['nome']}")

            if papel:
                st.markdown(
                    f'<span class="{membro["badge_class"]}">'
                    f'{papel}</span>',
                    unsafe_allow_html=True,
                )
            if desc_papel:
                st.markdown(
                    f'<p class="team-role-desc">{desc_papel}</p>',
                    unsafe_allow_html=True,
                )

            # ── Foto (container fixo 180×200) ──
            foto_up = st.file_uploader(
                f"📸 Foto de {membro['nome']}",
                type=['png', 'jpg', 'jpeg'],
                key=f"foto_{k}",
                help="Upload da foto de perfil (PNG, JPG, JPEG)",
            )
            _foto_b64_src = None
            if foto_up is not None:
                _raw = foto_up.read()
                dados_equipe[k]['foto'] = salvar_foto_base64(
                    _raw, f"{k}.jpg"
                )
                _foto_b64_src = (
                    'data:image/jpeg;base64,'
                    + base64.b64encode(_raw).decode()
                )
            elif dados_m.get('foto'):
                _foto_b64_src = (
                    'data:image/jpeg;base64,' + dados_m['foto']
                )

            if _foto_b64_src:
                st.markdown(
                    f'<div class="team-photo-box">'
                    f'<img src="{_foto_b64_src}" '
                    f'alt="{membro["nome"]}"/>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div class="team-photo-box">'
                    '<span class="team-photo-placeholder">'
                    '👤</span></div>',
                    unsafe_allow_html=True,
                )

            # ── Edição ──
            with st.expander(
                f"✏️ Editar informações", expanded=False
            ):
                with st.form(f"form_{k}"):
                    _papel = st.text_input(
                        "🎯 Papel no Projeto:",
                        value=dados_m.get('papel_projeto', ''),
                        key=f"papel_{k}",
                    )
                    _desc_papel = st.text_input(
                        "📝 Descrição do Papel:",
                        value=dados_m.get('descricao_papel', ''),
                        key=f"desc_papel_{k}",
                    )
                    _cargo = st.text_input(
                        "💼 Cargo:",
                        value=dados_m.get('cargo', ''),
                        key=f"cargo_{k}",
                    )
                    _empresa = st.text_input(
                        "🏢 Empresa:",
                        value=dados_m.get('empresa', ''),
                        key=f"empresa_{k}",
                    )
                    _exp = st.text_area(
                        "🎯 Experiência:",
                        value=dados_m.get('experiencia', ''),
                        key=f"exp_{k}",
                    )
                    _linkedin = st.text_input(
                        "🔗 LinkedIn:",
                        value=dados_m.get('linkedin', ''),
                        key=f"linkedin_{k}",
                    )
                    if st.form_submit_button(
                        "💾 Salvar", use_container_width=True
                    ):
                        dados_equipe[k]['papel_projeto'] = _papel
                        dados_equipe[k]['descricao_papel'] = _desc_papel
                        dados_equipe[k]['cargo'] = _cargo
                        dados_equipe[k]['empresa'] = _empresa
                        dados_equipe[k]['experiencia'] = _exp
                        dados_equipe[k]['linkedin'] = _linkedin
                        if salvar_dados_equipe(dados_equipe):
                            st.success("✅ Salvo com sucesso!")
                            st.rerun()

            # ── Perfil Profissional ──
            with st.expander("👨‍💻 Perfil Profissional", expanded=False):
                if dados_m.get('cargo') and dados_m.get('empresa'):
                    st.write(
                        f"💼 **{dados_m['cargo']}** "
                        f"na **{dados_m['empresa']}**"
                    )
                elif dados_m.get('cargo'):
                    st.write(f"💼 **{dados_m['cargo']}**")
                elif dados_m.get('empresa'):
                    st.write(f"🏢 **{dados_m['empresa']}**")
                else:
                    st.write("💼 *Cargo não informado*")

                if dados_m.get('experiencia'):
                    st.write(f"🎯 {dados_m['experiencia']}")
                else:
                    st.write("🎯 *Experiência não informada*")

                if dados_m.get('linkedin'):
                    st.markdown(
                        f"🔗 [Perfil no LinkedIn]"
                        f"({dados_m['linkedin']})"
                    )
                else:
                    st.write("🔗 *LinkedIn não informado*")

    st.markdown("---")
    
    st.markdown("""
    ### 🎯 Objetivos do Projeto

    O **Sistema TC** é uma plataforma de análise de custos industriais composta por dois módulos
    complementares, cada um atendendo um nível de granularidade diferente:

    **📊 TC Extendido (TC Ext)**
    - Análise de custos por oficina, conta e período
    - Visualização Normal (Custo Total) e CPU (Custo por Unidade)
    - Dashboard interativo com filtros (Ano, Período, Oficina, USI, Veículo)
    - Flex Budget: ajuste do orçamento pela proporção de volume realizado
    - Waterfall Analysis: decomposição de variações entre períodos
    - Exportação Excel completa com formatação profissional

    **🚗 TC Veículos (TC Principal)**
    - Cadeia completa: Despesa Primária → Custo FA → Custo FP → D&A → FP sem Dedicada
    - Rateio proporcional por veículo (tempo de produção)
    - 6 tabs especializadas: TC Veículos, Análise Flex, Volume, Custos por Oficina, Tempo de Produção, Dados Detalhados
    - Best Estimate: simulador de premissas (sensibilidade, inflação, volume) com geração de Forecast
    - Análise de Best Estimate: layout da Home alimentado por dados de Forecast

    **🔧 Capacidades Transversais**
    - 🚀 Cache inteligente com TTL e otimização de tipos de dados
    - 📦 Dados em formato Parquet comprimido
    - 💱 Conversão multi-moeda (BRL, USD, EUR) com taxas do banco de dados
    - 📊 Fator de escala configurável (Nenhum / K / M)
    - 🎨 Interface moderna com tabs, gráficos Altair e gradientes
    - ⚡ Performance otimizada para grandes volumes (70%+ redução de memória)
    """)

# ==========================================
# TC VEÍCULOS: REGRAS E CÁLCULO
# ==========================================
elif indice_selecionado == "📐 Regras e Cálculo" and modulo_doc == "🚗 TC Veículos":
    st.header("📐 Regras e Cálculo — TC Veículos")

    st.info(
        "📌 **Módulo TC Veículos** — Regras de cálculo específicas para "
        "análise de custo de produção de veículos."
    )

    with st.expander("💰 **Composição de Custos**", expanded=True):
        st.markdown("""
        ### 🔗 Cadeia de Custos TC Veículos

        ```
        Despesa Primária
          + Custo FA (Fluxo Anexo × Rateio FA)
          = Custo FP (Fabricação Principal)

        Custo FP = Despesa Primária + Custo FA
        D&A Dedicado = parcela de D&A atribuída diretamente ao veículo
        FP sem Dedicada = Custo FP − D&A Dedicado
        ```

        **Colunas Monetárias** (recebem conversão de moeda e fator):
        - `Despesa Primaria`, `Custo FA`, `Custo FP`, `D&A dedicado`, `FP sem Dedicada`

        **Redis** — Não é uma coluna. Identificado por linhas onde `Account = 'Redis'`:
        > Redis = Σ Despesa Primária onde Account = Redis
        """)

    with st.expander("🚗 **Rateio por Veículo**", expanded=False):
        st.markdown("""
        ### 📊 Processo de Rateio

        O custo da oficina é **rateado** aos veículos proporcionalmente ao **tempo de produção**:

        - **Percentual(v,o)** = TempoVeic(v,o) / Σ TempoVeic(v,o)
        - **CustoRateado(v,o)** = FPsemDedicada(o) × Percentual(v,o)
        - **CustoFPVeiculo(v,o)** = CustoRateado(v,o) + D&A Dedicado(v,o)

        **Dados Consolidados vs Rateados:**

        | Seleção | Fonte BUD | Fonte Real |
        |---------|-----------|------------|
        | Todos | `df_principal_BUD.parquet` | `df_principal.parquet` |
        | Veículo específico | `df_veiculos_custo_fp_BUD.parquet` | `df_veiculos_custo_fp.parquet` |

        > Quando **Veículo = "Todos"**: dados consolidados.
        > Quando **Veículo = modelo específico**: dados rateados com `Custo FP Veiculo`.
        """)

    with st.expander("📊 **Flex Budget**", expanded=False):
        st.markdown("""
        ### 🔄 Conceito

        O Budget Flex ajusta o orçamento pela proporção de volume realizado:
        - Custos **fixos** permanecem iguais ao Budget
        - Custos **variáveis** são ajustados pela proporção de volume

        ### 📐 Fórmulas

        - **Proporção** = Volume Realizado / Volume Budget
        - **Flex fixo** = BUD fixo (sem alteração)
        - **Flex variável** = BUD variável × Proporção
        - **Flex total** = Flex fixo + Flex variável

        ### 🏷️ Classificação Fixo/Variável

        A coluna `Custo` determina a classificação:
        - Valores que começam com `"Fix"` (case-insensitive) → **Fixo**
        - Todos os demais → **Variável**

        ```python
        mask_fixo = df['Custo'].str.lower().str.startswith('fix')
        ```
        """)

    with st.expander("📈 **CPU (Custo por Unidade)**", expanded=False):
        st.markdown("""
        ### 💲 Fórmula

        **CPU = Custo Total / Volume Total**

        Com proteção contra divisão por zero:
        ```python
        CPU = np.where(volume != 0, custo / volume, 0.0)
        ```

        **Quando o tipo de visualização é CPU:**
        - Cada métrica é dividida pelo volume total
        - O fator K/M **não é aplicado** (sempre "Nenhum")
        - Volumes de BUD e Actual são usados conforme o contexto
        """)

    with st.expander("🎯 **KPIs do TC Veículos**", expanded=False):
        st.markdown("""
        ### 📊 KPIs do Topo (fora das tabs)

        | KPI | Fórmula |
        |-----|---------|
        | Desp. Primária | Σ Despesa Primaria |
        | Custo FA | Σ Custo FA |
        | Redis | Σ Despesa Primaria (Account = Redis) |
        | Custo FP | Σ Custo FP |
        | D&A Dedicada | Σ D&A dedicado |
        | FP sem Dedicada | Σ FP sem Dedicada |

        ### 📊 KPIs do Resumo TC Veículos

        | KPI | Fórmula |
        |-----|---------|
        | BUD | BUD fixo + BUD variável |
        | Flex Bud − BUD | Flex total − BUD total |
        | Flex BUD | BUD fixo + BUD variável × Proporção |
        | Real − Flex Bud | Real total − Flex total |
        | Real | Σ Custo FP Real |
        | Real / Flex Bud | Real / Flex BUD (%) |
        """)

    with st.expander("🎯 **Arquitetura de Filtros**", expanded=False):
        st.markdown("""
        ### 🔍 Filtros do TC Veículos

        | Filtro | Tipo | Comportamento |
        |--------|------|---------------|
        | Oficina | multiselect | "Todos" ou seleção múltipla |
        | Tipo Custo | multiselect | Fixo/Variável ou todos |
        | Veículo | **selectbox** | "Todos" (consolidado) ou **1 veículo** (rateado) |
        | Período | multiselect | "Todos" ou seleção de meses |

        **Cascading:** A seleção de Oficina filtra os Veículos disponíveis:
        ```python
        _df_filt_ofi = df[df['Oficina'].isin(oficinas_selecionadas)]
        veiculos = sorted(_df_filt_ofi['Veículo'].dropna().unique())
        ```

        **Filtros globais:** Afetam KPIs, gráficos e Análise Flex simultaneamente.
        """)

    with st.expander("📈 **Sensibilidade e Volume (Best Estimate)**", expanded=False):
        st.markdown("""
        ### 🔮 Premissas do Simulador BE

        O Simulador de Best Estimate permite configurar premissas de **sensibilidade**, **inflação**
        e **volume** para projetar cenários futuros:

        **Fórmula Geral:**
        ```
        BE = Média_Histórica × Fator_Variação × Fator_Inflação
        ```

        Onde:
        - `Fator_Variação` = 1 + (Variação_Volume × Sensibilidade)
        - `Fator_Inflação` = 1 + (Inflação / 100)
        - `Variação_Volume` = (Volume_Futuro / Volume_Médio_Histórico) − 1

        **Sensibilidade (impacto do volume no custo):**
        - Controla o quanto a variação de volume afeta o custo
        - Pode ser configurada por oficina (Type 06) ou global
        - Custo Fixo: sensibilidade = 0% → custo não varia com o volume
        - Custo Variável: sensibilidade = 100% → custo varia proporcionalmente ao volume

        **Volume:**
        - Define o volume de produção projetado por veículo
        - Usado para calcular a variação de volume, Flex Budget e CPU do Forecast
        - Quando o custo não tem dimensão Veículo, o volume médio é usado diretamente (`.mean()`)
        - Quando há Veículo, o volume é somado por grupo (`.sum()`)

        **Inflação:**
        - Aplica % de reajuste sobre **todos** os custos (fixos e variáveis)
        - É aplicada **após** o ajuste por sensibilidade
        - Fórmula: `Custo_Final = Custo_Ajustado_Sensibilidade × (1 + Inflação/100)`

        **Resultado por tipo de custo:**
        - **Custo Fixo BE** = Média Histórica × (1 + Inflação%) — sem ajuste de volume
        - **Custo Variável BE** = Média Histórica × (Vol_Futuro / Vol_Histórico) × (1 + Inflação%)

        ### 📊 Geração de Forecast

        O simulador gera arquivos em `dados/TC_Principal/Forecast/`:
        - `forecast_completo.parquet` — Dados projetados mês a mês
        - `premissas.json` — Premissas utilizadas (sensibilidade, inflação, volume)

        Estes dados alimentam a página **Best Estimate (Análise)**, que usa o mesmo
        layout da Home (com gráficos e KPIs) mas com dados de Forecast.
        """)

# ==========================================
# SEÇÃO 2: REGRAS E CÁLCULO — TC EXTENDIDO
# ==========================================
elif indice_selecionado == "📐 Regras e Cálculo":
    st.header("📐 Regras e Cálculo — TC Extendido")

    st.warning(
        "⚠️ **Seção legada/complementar:** a referência oficial e atualizada de regras de cálculo "
        "(CPU/Flex Bud/ordem de conversões/contratos de dados) está em `DOCUMENTACAO_SISTEMA_TC.md` "
        "na seção **🧾 Especificação Técnica**."
    )
    st.button(
        "➡️ Ir para a Especificação Técnica",
        key="btn_ir_especificacao_regras",
        use_container_width=True,
        on_click=_ir_para_especificacao_tecnica,
    )
    
    st.markdown("""
    Esta seção documenta todas as regras de cálculo, filtros e metodologias utilizadas no projeto.
    **IMPORTANTE:** Esta documentação serve como referência para garantir que todos os cálculos sejam
    reproduzidos de forma idêntica, permitindo que a IA consulte e refaça qualquer cálculo do sistema.
    
    A documentação está organizada em expanders para facilitar a navegação. Cada seção contém explicações
    detalhadas das regras, fórmulas matemáticas completas e exemplos práticos para facilitar o entendimento.
    """)
    
    st.markdown("---")
    
    # EXPANDER 1: Cálculos Principais
    with st.expander("🔢 **Cálculos Principais e Métricas Fundamentais**", expanded=False):
        with st.expander("📊 **CPU (Custo por Unidade)**", expanded=False):
            st.markdown("""
            ### 📊 CPU (Custo por Unidade)
            
            O **CPU (Custo por Unidade)** é uma métrica fundamental que representa o custo médio por unidade de produção.
            É calculado dividindo o custo total pelo volume de produção.
            
            **Fórmula Matemática:**
            ```
            CPU = Custo_Total / Volume_Total
            ```
            
            Onde:
            - `Custo_Total` = Soma de todos os custos individuais após agrupamento
            - `Volume_Total` = Soma de todos os volumes após agrupamento
            
            **⚠️ REGRA CRÍTICA:** O CPU deve ser calculado **APÓS** o agrupamento dos dados, nunca antes.
            Esta é uma das regras mais importantes do sistema, pois calcular CPU antes de agrupar resulta em valores incorretos.
            
            **Por que calcular após agrupamento?**
            
            A média aritmética de CPUs individuais não é igual ao CPU do total agregado. Isso ocorre porque o CPU é uma
            razão (divisão), e a média de razões não é igual à razão das médias.
            
            **Exemplo Ilustrativo:**
            
            Considere duas linhas de dados:
            - **Linha 1:** Custo Total = R$ 100, Volume = 10 unidades -> CPU = R$ 10,00/unidade
            - **Linha 2:** Custo Total = R$ 200, Volume = 40 unidades -> CPU = R$ 5,00/unidade
            
            **Método Incorreto (calcular CPU antes de agrupar):**
            - CPU médio = (R$ 10,00 + R$ 5,00) / 2 = **R$ 7,50/unidade** [INCORRETO]
            
            **Método Correto (calcular CPU após agrupar):**
            - Custo Total Agregado = R$ 100 + R$ 200 = R$ 300
            - Volume Total Agregado = 10 + 40 = 50 unidades
            - CPU Agregado = R$ 300 / 50 = **R$ 6,00/unidade** [CORRETO]
            
            A diferença entre R$ 7,50 e R$ 6,00 pode parecer pequena, mas em grandes volumes de dados essa discrepância
            se acumula e resulta em análises completamente incorretas.
            
            **Interpretação do CPU:**
            - **CPU baixo:** Indica eficiência operacional, menor custo por unidade produzida
            - **CPU alto:** Indica ineficiência ou custos elevados por unidade produzida
            - **Variação de CPU:** Mudanças no CPU entre períodos indicam variações na eficiência operacional
            """)
        
        with st.expander("💰 **Custo Total**", expanded=False):
            st.markdown("""
            ### 💰 Custo Total
        
        O **Custo Total** representa a soma de todos os custos individuais após a aplicação de filtros e agrupamentos.
        
        **Fórmula Matemática:**
        ```
        Custo_Total = Σ(Custo_Individual)
        ```
        
        Onde `Σ` representa a soma de todos os custos individuais que atendem aos critérios de filtragem.
        
        **Regras de Cálculo:**
        - Sempre somar valores individuais, nunca calcular média
        - Aplicar todos os filtros antes de realizar o agrupamento
        - Considerar apenas valores que atendem aos critérios de seleção
        - Não incluir valores nulos ou zerados no cálculo
        
        **Agrupamento por Dimensões:**
        
        O custo total pode ser calculado para diferentes níveis de agregação:
        - Por período (mês, trimestre, semestre, ano)
        - Por oficina
        - Por veículo
        - Por categoria de custo (Type 05, Type 06, Account)
        - Por combinação de dimensões
        
        **Interpretação:**
        - **Custo Total crescente:** Indica aumento nos gastos operacionais
        - **Custo Total decrescente:** Indica redução nos gastos operacionais
        - **Comparação entre períodos:** Permite identificar tendências e variações
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 🔄 Fator de Conversão (K/M)
        
        Os **Fatores de Conversão** são utilizados para facilitar a visualização de valores muito grandes,
        convertendo-os para unidades mais legíveis (milhares ou milhões).
        
        **Fatores Disponíveis:**
        - **K (milhares):** Divide o valor por 1.000
        - **M (Milhões):** Divide o valor por 1.000.000
        - **Nenhum:** Mantém o valor original
        
        **Fórmulas Matemáticas:**
        ```
        Valor_K = Valor_Original / 1.000
        Valor_M = Valor_Original / 1.000.000
        ```
        
        **⚠️ REGRA CRÍTICA:** O fator de conversão **NÃO** deve ser aplicado no modo **CPU (Custo por Unidade)**.
        
        **Por que não aplicar em CPU?**
        
        O CPU já é uma razão (divisão entre Custo Total e Volume). Se aplicarmos o fator de conversão ao Custo Total
        antes de calcular o CPU, estaríamos dividindo duas vezes, o que resultaria em valores completamente incorretos.
        
        **Exemplo:**
        - Custo Total Original: R$ 1.000.000
        - Volume: 10.000 unidades
        - CPU Correto: R$ 1.000.000 / 10.000 = **R$ 100,00/unidade** [CORRETO]
        
        Se aplicássemos o fator K antes:
        - Custo Total com K: R$ 1.000 K
        - CPU Incorreto: R$ 1.000 K / 10.000 = **R$ 0,10/unidade** [INCORRETO] (1000 vezes menor!)
        
        **Ordem de Aplicação das Transformações:**
        
        1. **Primeiro:** Aplicar fator de conversão (K/M) - apenas em modo Custo Total
        2. **Segundo:** Converter moeda (se necessário)
        3. **Terceiro:** Realizar cálculos (CPU, Flex Bud, diferenças, etc.)
        
        Esta ordem garante que todas as transformações sejam aplicadas corretamente e que os resultados finais
        sejam consistentes e precisos.
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 📅 Agrupamento por Período
        
        O **Agrupamento por Período** permite consolidar dados em diferentes intervalos de tempo, facilitando
        análises comparativas e identificação de tendências.
        
        **Estrutura de Períodos:**
        
        Quando os dados contêm informação de **Ano**, o sistema cria uma coluna combinada `Período_Ano` que
        agrupa tanto o período quanto o ano:
        ```
        Período_Ano = Período + " " + Ano
        ```
        
        Exemplo: "Janeiro 2024", "Fevereiro 2024", etc.
        
        **Agrupamento com Ano:**
        - Dimensões de agrupamento: `['Ano', 'Período']`
        - Permite comparações ano a ano
        - Facilita análises de tendências de longo prazo
        
        **Agrupamento sem Ano:**
        - Dimensões de agrupamento: `['Período']`
        - Útil quando todos os dados são do mesmo ano
        - Simplifica análises mensais ou trimestrais
        
        **Fórmula de Agregação:**
        ```
        Custo_Total_Agrupado = Σ(Custo_Individual) agrupado por Período
        Volume_Total_Agrupado = Σ(Volume_Individual) agrupado por Período
        ```
        
        **Interpretação:**
        - Permite identificar sazonalidades e padrões temporais
        - Facilita comparações entre períodos equivalentes
        - Suporta análises de tendências e projeções
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 📈 Cálculo de Diferenças e Ratios
        
        As **Diferenças e Ratios** são métricas essenciais para análise de desempenho, permitindo comparar
        valores reais com valores planejados ou ajustados.
        
        **1. Diferença Flex Bud - BUD:**
        
        Esta métrica compara o Budget Flexível (ajustado pelo volume real) com o Budget original planejado.
        
        **Fórmula:**
        ```
        Delta_Flex_Bud = Flex_BUD - BUD
        ```
        
        **Interpretação:**
        - **Valor positivo:** Flex Bud > BUD (custo ajustado maior que o planejado)
        - **Valor negativo:** Flex Bud < BUD (custo ajustado menor que o planejado)
        - **Zero:** Flex Bud = BUD (custo ajustado igual ao planejado)
        
        **2. Diferença Total - Flex Bud:**
        
        Esta métrica compara o custo real com o Budget Flexível, indicando a eficiência operacional.
        
        **Fórmula:**
        ```
        Delta_Total_Flex = Total - Flex_BUD
        ```
        
        **Interpretação:**
        - **Valor positivo:** Total > Flex Bud (ineficiência operacional)
        - **Valor negativo:** Total < Flex Bud (eficiência operacional)
        - **Zero:** Total = Flex Bud (desempenho exatamente como esperado)
        
        **3. Ratio Total / Flex Bud:**
        
        Esta métrica expressa o desempenho real como percentual do Budget Flexível.
        
        **Fórmula:**
        ```
        Ratio = Total / Flex_BUD
        Percentual = Ratio * 100%
        ```
        
        **Interpretação:**
        - **< 100%:** Total < Flex Bud (melhor que esperado, eficiência operacional)
        - **= 100%:** Total = Flex Bud (exatamente como esperado)
        - **> 100%:** Total > Flex Bud (pior que esperado, ineficiência operacional)
        
        **Exemplo Prático:**
        - Flex Bud = R$ 500.000
        - Total Real = R$ 520.000
        - Ratio = 520.000 / 500.000 = 1,04 = **104%**
        - Interpretação: O custo real está 4% acima do Budget Flexível, indicando ineficiência operacional
        """)
    
    # EXPANDER 2: Flex Bud
    with st.expander("🔄 **Cálculo de Flex Bud (Budget Flexível)**", expanded=False):
        with st.expander("📋 **Conceito e Regras Fundamentais**", expanded=False):
            st.markdown("""
            ### Conceito
            
            **Flex Bud** (Budget Flexível) é um valor ajustado que considera a variação de volume,
            aplicando regras diferentes para custos fixos e **não‑fixos**.
            
            **IMPORTANTE:** Existem dois contextos diferentes de cálculo:
            1. **Real x Real** (Waterfall): Compara dois períodos reais (Mês 1 vs Mês 2)
            2. **Real x Budget** (TC Ext): Compara período real vs budget planejado
            """)
            
            st.markdown("---")
            
            st.markdown("## 📋 Regras Fundamentais: Fixo vs Não‑Fixo")
            
            st.markdown("""
            ### Regra Geral para Custos Fixos
            
            **Princípio:** Custos fixos NÃO variam com o volume de produção.
            
            **Fórmula Geral:**
            ```
            Flex_Fixo = Valor_Original_Fixo
            ```
            
            **Explicação:**
            - Independente da variação de volume, o custo fixo permanece constante
            - Exemplos: Aluguel, salários fixos, depreciação
            - Sensibilidade ao volume: **0%** (zero por cento)
            """)
            
            st.markdown("---")
            
            st.markdown("""
            ### Regra Geral para Custos Não‑Fixos
            
            **Princípio:** Custos **não‑fixos** variam PROPORCIONALMENTE ao volume de produção.
            
            **Fórmula Geral:**
            ```
            Flex_NãoFixo = Valor_Original_NãoFixo * (Volume_Novo / Volume_Original)
            ```
            
            **Explicação:**
            - Se o volume dobra, o custo **não‑fixo** escala proporcionalmente
            - Se o volume reduz pela metade, o custo **não‑fixo** escala proporcionalmente
            - Exemplos: componentes variáveis e demais classificações que não sejam Fixo
            - Sensibilidade ao volume: **100%** (cem por cento)
            """)
        
        # Ler o conteúdo do Flex Bud que está mais abaixo no arquivo
        # Por enquanto, vou adicionar um placeholder e depois mover o conteúdo correto
        st.info("📚 Conteúdo detalhado do Flex Bud será movido para cá...")
    
    # EXPANDER 3: Volumes
    with st.expander("📊 **Cálculo de Volumes**", expanded=False):
        with st.expander("📁 **Fonte de Dados de Volume**", expanded=False):
            st.markdown("""
            ### 📁 Fonte de Dados de Volume
            
            Os dados de volume são armazenados em arquivos Parquet otimizados para garantir performance e eficiência
            no processamento. O sistema utiliza diferentes arquivos de volume dependendo do contexto de análise.
            
            **Arquivos de Volume:**
            
            - **`df_vol_historico.parquet`:** Dados históricos consolidados de volume
            - **`df_vol.parquet`:** Dados de volume por ano específico
            - **`df_vol_historico_BUD.parquet`:** Dados de volume do budget histórico
            
            **Estrutura dos Dados de Volume:**
            
            Os arquivos de volume contêm as seguintes colunas obrigatórias:
            - **`Volume`:** Quantidade de unidades produzidas
            - **`Período`:** Período de referência (mês, trimestre, etc.)
            - **`Oficina`:** Identificação da oficina
            - **`Veículo`:** Identificação do veículo
            
            E a seguinte coluna opcional:
            - **`Ano`:** Ano de referência (quando disponível)
            
            **Sincronização com Dados de Custo:**
            
            Os dados de volume são estruturados de forma a permitir sincronização perfeita com os dados de custo,
            garantindo que o cálculo de CPU seja preciso e consistente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Filtragem de Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Filtragem de Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta regra é absolutamente crítica para garantir a precisão do cálculo de CPU.
            
            **Processo de Filtragem:**
            
            O processo de filtragem de volumes segue os seguintes passos:
            
            1. **Criar conjunto filtrado:** Iniciar com uma cópia dos dados de volume originais
            2. **Aplicar filtros sincronizados:** Usar os valores únicos extraídos dos dados de custo filtrados
            3. **Filtrar por período:** Aplicar filtro específico para o período de análise
            4. **Agrupar e somar:** Agrupar por período e somar os volumes
            
            **Fórmula de Agregação:**
            
            ```
            Volume_Total = Σ(Volume_Individual) agrupado por Período
            ```
            
            Onde `Σ` representa a soma de todos os volumes individuais que atendem aos critérios de filtragem.
            
            **Importância da Consistência:**
            
            A consistência entre os filtros aplicados aos dados de custo e volume é essencial porque:
            - O CPU é calculado como Custo Total / Volume Total
            - Se os filtros forem diferentes, o CPU resultante será completamente incorreto
            - Análises baseadas em CPU incorreto podem levar a decisões de negócio equivocadas
            
            **Exemplo de Impacto:**
            
            Se você filtrar os custos para uma oficina específica mas não filtrar os volumes da mesma forma:
            - Custo Total (filtrado): R$ 50.000 (apenas Oficina A)
            - Volume Total (não filtrado): 100.000 unidades (todas as oficinas)
            - CPU Incorreto: R$ 50.000 / 100.000 = R$ 0,50/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 50.000 (Oficina A)
            - Volume Total (filtrado): 10.000 unidades (Oficina A)
            - CPU Correto: R$ 50.000 / 10.000 = R$ 5,00/unidade [CORRETO]
            """)
    
    # EXPANDER 4: Moeda e Taxas
    with st.expander("💱 **Moeda e Taxas de Câmbio**", expanded=False):
        with st.expander("💱 **Moedas Suportadas**", expanded=False):
            st.markdown("""
            ### 💱 Moedas Suportadas
            
            O sistema suporta conversão entre diferentes moedas para facilitar análises internacionais e comparações
            com dados de outras unidades de negócio. As moedas disponíveis são:
            
            - **BRL (R$):** Real Brasileiro - moeda base do sistema
            - **USD ($):** Dólar Americano
            - **EUR:** Euro
            
            **Moeda Base:**
            
            O Real Brasileiro (BRL) é a moeda base do sistema. Todos os valores são originalmente armazenados em BRL,
            e as conversões para outras moedas são realizadas multiplicando os valores pela taxa de câmbio correspondente.
            """)
        
        with st.expander("📊 **Taxas de Câmbio**", expanded=False):
            st.markdown("""
            ### 📊 Taxas de Câmbio
            
            As **Taxas de Câmbio** definem a relação de conversão entre a moeda base (BRL) e as outras moedas suportadas.
            
            **Definição Matemática:**
            
            As taxas são definidas como a quantidade de moeda estrangeira equivalente a 1 Real Brasileiro:
            ```
            1 BRL = Taxa_USD USD
            1 BRL = Taxa_EUR EUR
            ```
            
            **Exemplo Prático:**
            
            Se a taxa de câmbio USD for 0,20, isso significa que:
            - 1 Real Brasileiro = 0,20 Dólares Americanos
            - Para converter R$ 100,00 para USD: R$ 100,00 * 0,20 = $ 20,00
            
            **Fórmula de Conversão:**
            
            Para converter um valor de BRL para outra moeda:
            ```
            Valor_Convertido = Valor_BRL * Taxa_Cambio
            ```
            
            Onde:
            - `Valor_BRL` = Valor original em Real Brasileiro
            - `Taxa_Câmbio` = Taxa de câmbio da moeda de destino
            - `Valor_Convertido` = Valor convertido para a moeda de destino
            
            **Ordem de Aplicação das Transformações:**
            
            Quando múltiplas transformações são aplicadas (fator de conversão K/M e conversão de moeda), a ordem
            é crítica para garantir resultados corretos:
            
            1. **Primeiro:** Aplicar fator de conversão (K/M) - apenas em modo Custo Total
            2. **Segundo:** Converter moeda (se necessário)
            3. **Terceiro:** Realizar cálculos (CPU, Flex Bud, diferenças, etc.)
            
            **Exemplo Completo de Transformação:**
            
            Considere um valor original de R$ 1.000.000,00:
            
            - **Passo 1 (Fator K):** R$ 1.000.000,00 / 1.000 = R$ 1.000 K
            - **Passo 2 (Conversão USD, taxa 0,20):** R$ 1.000 K * 0,20 = $ 200 K
            - **Resultado Final:** $ 200 K (duzentos mil dólares)
            """)
        
        with st.expander("💾 **Persistência e Atualização de Taxas**", expanded=False):
            st.markdown("""
            ### 💾 Persistência e Atualização de Taxas
            
            As taxas de câmbio são armazenadas de forma persistente para garantir que as conversões sejam
            consistentes entre diferentes sessões de análise.
            
            **Armazenamento:**
            
            - As taxas são salvas em banco de dados ou arquivo de configuração
            - Valores padrão são utilizados caso não existam taxas salvas
            - As taxas podem ser atualizadas a qualquer momento através da interface do sistema
            
            **Atualização de Taxas:**
            
            As taxas de câmbio podem ser atualizadas para refletir as condições de mercado atuais. Quando uma
            nova taxa é definida, ela é aplicada a todos os cálculos subsequentes, garantindo que as análises
            estejam sempre baseadas nas taxas mais recentes.
            
            **Importância da Atualização:**
            
            Manter as taxas de câmbio atualizadas é essencial para garantir a precisão das análises, especialmente
            em períodos de alta volatilidade cambial. Taxas desatualizadas podem resultar em comparações e
            análises completamente incorretas.
            """)
    
    # EXPANDER 5: Filtros e Perímetros
    with st.expander("🔍 **Filtros e Perímetros de Análise**", expanded=False):
        with st.expander("🎯 **Sistema de Filtros da Interface**", expanded=False):
            st.markdown("""
            ### 🎯 Sistema de Filtros da Interface
            
            O sistema possui um conjunto abrangente de filtros que permitem refinar a análise de dados de forma
            precisa e flexível. Os filtros são aplicados sequencialmente, criando um perímetro de análise cada vez
            mais específico conforme o usuário seleciona diferentes critérios.
            
            **Ordem de Aplicação dos Filtros:**
            
            Os filtros são aplicados na seguinte ordem hierárquica, garantindo que cada filtro refine o resultado
            do filtro anterior:
            
            1. **Ano** - Seleção do ano de análise (Radio button)
            2. **Oficina** - Seleção de uma ou mais oficinas (Multiselect)
            3. **Veículo** - Seleção de um ou mais veículos (Multiselect)
            4. **USI** - Seleção de unidades de serviço (Multiselect)
            5. **Período** - Seleção de período específico (Selectbox)
            6. **Centro cst** - Seleção de centro de custo (Selectbox)
            7. **Conta contábil** - Seleção de contas contábeis (Multiselect)
            8. **Type 5** - Seleção de categorias Type 05 (Multiselect)
            9. **Type 6** - Seleção de categorias Type 06 (Multiselect)
            10. **Fornecedor** - Seleção de fornecedores (Multiselect)
            11. **Fornec.** - Seleção adicional de fornecedores (Multiselect)
            12. **Tipo** - Seleção de tipos de custo (Multiselect)
            13. **Filtros Avançados:**
                - **Usuário** - Filtro por usuário responsável
                - **Material** - Filtro por material utilizado
                - **Dt.lçto.** - Filtro por data de lançamento
                - **Texto breve** - Filtro por texto descritivo
                - **Account** - Filtro por conta contábil específica
            
            **Princípio de Funcionamento:**
            
            Cada filtro atua como um "funil" que reduz progressivamente o conjunto de dados analisados. Quando
            múltiplos filtros são aplicados, apenas os registros que atendem a **TODOS** os critérios selecionados
            são incluídos na análise final.
            
            **Exemplo de Aplicação Sequencial:**
            
            Imagine que você selecionou:
            - Ano: 2024
            - Oficina: "Oficina A" e "Oficina B"
            - Veículo: "Veículo X"
            - Período: "Janeiro"
            
            O sistema primeiro filtra todos os dados de 2024, depois mantém apenas os registros das Oficinas A e B,
            em seguida mantém apenas os registros do Veículo X, e finalmente mantém apenas os registros de Janeiro.
            O resultado final contém apenas os registros que atendem a todos esses critérios simultaneamente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Perímetro de Filtros para Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Perímetro de Filtros para Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta é uma das regras mais importantes do sistema, pois garante que o cálculo de
            CPU seja preciso e consistente.
            
            **Por que esta regra é crítica?**
            
            O CPU é calculado como a razão entre Custo Total e Volume. Se os filtros aplicados ao custo forem
            diferentes dos filtros aplicados ao volume, o CPU resultante será completamente incorreto.
            
            **Exemplo Ilustrativo:**
            
            Imagine que você filtrou os dados de custo para incluir apenas:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            - Período: "Janeiro"
            
            Se o volume não for filtrado da mesma forma, você poderia estar dividindo:
            - Custo Total (filtrado): R$ 100.000 (apenas Oficina A, Veículo X, Janeiro)
            - Volume Total (não filtrado): 50.000 unidades (todas as oficinas, todos os veículos, todos os períodos)
            - CPU Incorreto: R$ 100.000 / 50.000 = R$ 2,00/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 100.000 (Oficina A, Veículo X, Janeiro)
            - Volume Total (filtrado): 10.000 unidades (Oficina A, Veículo X, Janeiro)
            - CPU Correto: R$ 100.000 / 10.000 = R$ 10,00/unidade [CORRETO]
            
            **Mecanismo de Sincronização:**
            
            O sistema garante a sincronização dos filtros extraindo os valores únicos das dimensões filtradas dos
            dados principais e aplicando esses mesmos valores aos dados de volume. Isso garante que o perímetro de
            análise seja idêntico para ambos os conjuntos de dados.
            
            **Dimensões Sincronizadas:**
            
            As seguintes dimensões são sempre sincronizadas entre dados de custo e volume:
            - Veículo
            - Oficina
            - USI
            - Centro de Custo
            - Conta Contábil
            - Type 05
            - Type 06
            - Fornecedor
            - Tipo
            - E todos os filtros avançados (Usuário, Material, Data, etc.)
            """)
        
        with st.expander("📊 **Sincronização de Filtros para Budget**", expanded=False):
            st.markdown("""
            ### 📊 Sincronização de Filtros para Budget
            
            **Regra Fundamental:** O Budget deve usar os mesmos filtros aplicados aos dados reais para garantir
            comparações justas e precisas.
            
            **Por que sincronizar filtros do Budget?**
            
            Quando comparamos dados reais com budget, precisamos garantir que estamos comparando "maçãs com maçãs".
            Se os dados reais estão filtrados para uma oficina específica, o budget também deve estar filtrado para
            a mesma oficina, caso contrário a comparação não terá sentido.
            
            **Exemplo:**
            
            Se você filtrar os dados reais para:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            
            O budget também será automaticamente filtrado para:
            - Oficina: "Oficina A"
            - Veículo: "Veículo X"
            
            Isso garante que a comparação entre Real e Budget seja feita no mesmo contexto operacional.
            
            **Mecanismo de Aplicação:**
            
            O sistema extrai os valores únicos de todas as dimensões filtradas dos dados reais e aplica esses mesmos
            valores como filtros ao budget. Isso garante que o perímetro de análise seja idêntico para ambos os
            conjuntos de dados, permitindo comparações precisas e significativas.
            """)

    # EXPANDER 3: Volumes
    with st.expander("📊 **Cálculo de Volumes**", expanded=False):
        with st.expander("📁 **Fonte de Dados de Volume**", expanded=False):
            st.markdown("""
            ### 📁 Fonte de Dados de Volume
            
            Os dados de volume são armazenados em arquivos Parquet otimizados para garantir performance e eficiência
            no processamento. O sistema utiliza diferentes arquivos de volume dependendo do contexto de análise.
            
            **Arquivos de Volume:**
            
            - **`df_vol_historico.parquet`:** Dados históricos consolidados de volume
            - **`df_vol.parquet`:** Dados de volume por ano específico
            - **`df_vol_historico_BUD.parquet`:** Dados de volume do budget histórico
            
            **Estrutura dos Dados de Volume:**
            
            Os arquivos de volume contêm as seguintes colunas obrigatórias:
            - **`Volume`:** Quantidade de unidades produzidas
            - **`Período`:** Período de referência (mês, trimestre, etc.)
            - **`Oficina`:** Identificação da oficina
            - **`Veículo`:** Identificação do veículo
            
            E a seguinte coluna opcional:
            - **`Ano`:** Ano de referência (quando disponível)
            
            **Sincronização com Dados de Custo:**
            
            Os dados de volume são estruturados de forma a permitir sincronização perfeita com os dados de custo,
            garantindo que o cálculo de CPU seja preciso e consistente.
            """)
        
        with st.expander("⚠️ **REGRA CRÍTICA: Filtragem de Volumes**", expanded=False):
            st.markdown("""
            ### ⚠️ REGRA CRÍTICA: Filtragem de Volumes
            
            **Princípio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta regra é absolutamente crítica para garantir a precisão do cálculo de CPU.
            
            **Processo de Filtragem:**
            
            O processo de filtragem de volumes segue os seguintes passos:
            
            1. **Criar conjunto filtrado:** Iniciar com uma cópia dos dados de volume originais
            2. **Aplicar filtros sincronizados:** Usar os valores únicos extraídos dos dados de custo filtrados
            3. **Filtrar por período:** Aplicar filtro específico para o período de análise
            4. **Agrupar e somar:** Agrupar por período e somar os volumes
            
            **Fórmula de Agregação:**
            
            ```
            Volume_Total = Σ(Volume_Individual) agrupado por Período
            ```
            
            Onde `Σ` representa a soma de todos os volumes individuais que atendem aos critérios de filtragem.
            
            **Importância da Consistência:**
            
            A consistência entre os filtros aplicados aos dados de custo e volume é essencial porque:
            - O CPU é calculado como Custo Total / Volume Total
            - Se os filtros forem diferentes, o CPU resultante será completamente incorreto
            - Análises baseadas em CPU incorreto podem levar a decisões de negócio equivocadas
            
            **Exemplo de Impacto:**
            
            Se você filtrar os custos para uma oficina específica mas não filtrar os volumes da mesma forma:
            - Custo Total (filtrado): R$ 50.000 (apenas Oficina A)
            - Volume Total (não filtrado): 100.000 unidades (todas as oficinas)
            - CPU Incorreto: R$ 50.000 / 100.000 = R$ 0,50/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 50.000 (Oficina A)
            - Volume Total (filtrado): 10.000 unidades (Oficina A)
            - CPU Correto: R$ 50.000 / 10.000 = R$ 5,00/unidade [CORRETO]
            """)
    
    st.markdown("---")
    
    st.markdown("## 📋 Regras Fundamentais: Fixo vs Não‑Fixo")
    
    st.markdown("""
    ### Regra Geral para Custos Fixos
    
    **Princípio:** Custos fixos NÃO variam com o volume de produção.
    
    **Fórmula Geral:**
    ```
    Flex_Fixo = Valor_Original_Fixo
    ```
    
    **Explicação:**
    - Independente da variação de volume, o custo fixo permanece constante
    - Exemplos: Aluguel, salários fixos, depreciação
    - Sensibilidade ao volume: **0%** (zero por cento)
    
    **Implementação:**
    ```python
    # Sempre manter o valor original
    flex_fixo = custo_fixo_original
    ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Regra Geral para Custos Variáveis
    
    **Princípio:** Custos variáveis variam PROPORCIONALMENTE ao volume de produção.
    
    **Fórmula Geral:**
    ```
    Flex_NãoFixo = Valor_Original_NãoFixo * (Volume_Novo / Volume_Original)
    ```
    
    **Explicação:**
    - Se o volume dobra, o custo variável dobra
    - Se o volume reduz pela metade, o custo variável reduz pela metade
    - Exemplos: Matéria-prima, energia variável, comissões
    - Sensibilidade ao volume: **100%** (cem por cento)
    
    **Implementação:**
    ```python
    # Calcular proporção de volume
    proporcao = volume_novo / volume_original
    
    # Aplicar proporção ao custo variável
    flex_variavel = custo_variavel_original * proporcao
    ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Identificação de Fixo vs Variável
    
    **Coluna 'Custo' no DataFrame:**
    - Deve conter os valores: `'Fixo'` ou `'Variável'`
    - Cada linha de dados deve ter esta classificação
    
    **Implementação:**
    ```python
    # Separar Fixo e Variável
    if 'Custo' in df.columns:
        custo_fixo = df[df['Custo'] == 'Fixo']['Total'].sum()
        custo_variavel = df[df['Custo'] == 'Variável']['Total'].sum()
    else:
        # Se não tiver coluna Custo, assumir tudo como variável
        custo_fixo = 0
        custo_variavel = df['Total'].sum()
    ```
    """)
    
    st.markdown("---")
    
    # Sub-seções para separar os dois casos
    st.markdown("## 📊 CASO 1: Flex para Comparação Real x Real (Waterfall)")
    
    st.markdown("""
    ### Contexto
    
    Usado na página **1 - Waterfall** para comparar dois períodos reais:
    - **Mês 1** (período inicial real)
    - **Mês 2** (período final real)
    
    **Objetivo:** Calcular o que seria o custo do Mês 1 ajustado pelo volume do Mês 2.
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Regras de Cálculo - Real x Real
        
        **Passo 1: Identificar Custos do Mês 1**
        ```python
        # Separar Fixo e Variável do Mês 1
        C1_Fixo = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum()
        C1_Variavel = df_m1[df_m1['Custo'] == 'Variável']['Total'].sum()
        C1_Total = C1_Fixo + C1_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V1 = volume_real_mes1  # Volume do Mês 1
        V2 = volume_real_mes2  # Volume do Mês 2
        ```
        
        **Passo 3: Calcular Proporção de Volume**
        ```python
        rho = V2 / V1  # Proporção de volume
        ```
        
        **Passo 4: Aplicar Regras de Fixo e Variável**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo não varia com volume
        Flex_Mes1_Fixo = C1_Fixo
        # Explicação: Mantém o valor original, independente da variação de volume
        ```
        **Fórmula Matemática:**
        ```
        Flex_Mês1_Fixo = C_1_Fixo
        ```
        **Por que não multiplica pela proporção?**
        - Custos fixos são independentes do volume de produção
        - Exemplos: Aluguel, salários fixos, depreciação
        - Mesmo que o volume dobre, o custo fixo permanece igual
        
        **Para Custo Variável:**
        ```python
        # REGRA: Variável varia proporcionalmente ao volume
        Flex_Mes1_Variavel = C1_Variavel * rho
                             = C1_Variavel * (V2 / V1)
        # Explicação: Multiplica pelo fator de proporção de volume
        ```
        **Fórmula Matemática:**
        ```
        Flex_Mês1_Variável = C_1_Variável * rho
                              = C_1_Variável * (V_2 / V_1)
        ```
        **Por que multiplica pela proporção?**
        - Custos variáveis variam proporcionalmente ao volume
        - Se o volume dobra, o custo variável dobra
        - Se o volume reduz pela metade, o custo variável reduz pela metade
        - Exemplos: Matéria-prima, energia variável, comissões
        
        **Passo 5: Calcular Flex Mês 1 Total**
        ```python
        Flex_Mes1_Total = Flex_Mes1_Fixo + Flex_Mes1_Variavel
                         = C1_Fixo + (C1_Variavel * rho)
        ```
        **Fórmula Matemática:**
        ```
        Flex_Mês1_Total = Flex_Mês1_Fixo + Flex_Mês1_Variável
                        = C_1_Fixo + (C_1_Variável * rho)
                        = C_1_Fixo + C_1_Variável * (V_2 / V_1)
        ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Fórmulas Matemáticas Completas - Real x Real
    
    **Definições:**
    - `V_1` = Volume Real do Mês 1
    - `V_2` = Volume Real do Mês 2
    - `C_1_Fixo` = Custo Total Fixo do Mês 1
    - `C_1_Variável` = Custo Total Variável do Mês 1
    - `C_1_Total` = Custo Total do Mês 1 = `C_1_Fixo + C_1_Variável`
    
    **Proporção de Volume:**
    ```
    rho = V_2 / V_1
    ```
    Onde:
    - `rho > 1` significa que o volume aumentou
    - `rho < 1` significa que o volume diminuiu
    - `rho = 1` significa que o volume permaneceu igual
    
    **Cálculo de Flex Mês 1 (em Custo Total):**
    
    Para **Custo Fixo:**
    ```
    Flex_Mês1_Fixo = C_1_Fixo
    ```
    **Regra Aplicada:** Fixo não varia com volume
    - Valor original mantido: `C_1_Fixo`
    - Não multiplica pela proporção de volume
    - Motivo: Custos fixos são independentes do volume de produção
    
    Para **Custo Variável:**
    ```
    Flex_Mês1_Variável = C_1_Variável * rho
                          = C_1_Variável * (V_2 / V_1)
    ```
    **Regra Aplicada:** Variável varia proporcionalmente ao volume
    - Valor original: `C_1_Variável`
    - Multiplica pela proporção: `rho = V_2 / V_1`
    - Motivo: Custos variáveis aumentam/diminuem na mesma proporção do volume
    
    **Flex Mês 1 Total (em Custo Total):**
    ```
    Flex_Mês1_Total = Flex_Mês1_Fixo + Flex_Mês1_Variável
                    = C_1_Fixo + (C_1_Variável * rho)
                    = C_1_Fixo + C_1_Variável * (V_2 / V_1)
    ```
    **Regra Aplicada:** Soma do Fixo (inalterado) + Variável (ajustado)
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Cálculo em CPU (Custo por Unidade) - Real x Real
    
    **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD (Mês 1) em CPU:**
        ```
        BUD_CPU = C_1_Total / V_1
                 = (C_1_Fixo + C_1_Variável) / V_1
        ```
        
        **Flex Mês 1 em CPU:**
        ```
        Flex_Mês1_CPU = Flex_Mês1_Total / V_2
                       = [C_1_Fixo + C_1_Variável * (V_2 / V_1)] / V_2
                       = (C_1_Fixo / V_2) + (C_1_Variável / V_1)
        ```
        
        **Diferença (Flex Mês 1 - Mês 1):**
        ```
        Delta_Flex = Flex_Mês1_CPU - BUD_CPU
               = [(C_1_Fixo / V_2) + (C_1_Variável / V_1)] - [(C_1_Fixo + C_1_Variável) / V_1]
               = (C_1_Fixo / V_2) - (C_1_Fixo / V_1)
               = C_1_Fixo * (1/V_2 - 1/V_1)
               = C_1_Fixo * (V_1 - V_2) / (V_1 * V_2)
        ```
        
        **Interpretação:**
        - Se `V_2 > V_1`: `Delta_Flex < 0` (CPU diminui porque custo fixo é diluído em mais volume)
        - Se `V_2 < V_1`: `Delta_Flex > 0` (CPU aumenta porque custo fixo é concentrado em menos volume)
        - Se `V_2 = V_1`: `Delta_Flex = 0` (sem variação)
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Implementação - Real x Real
        
        ```python
        # 1. Obter dados do Mês 1
        df_m1 = df_filtrado[df_filtrado['Período'] == mes_inicial]
        
        # 2. Separar Fixo e Variável
        if 'Custo' in df_m1.columns:
            C1_Fixo = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum()
            C1_Variavel = df_m1[df_m1['Custo'] == 'Variável']['Total'].sum()
        else:
            C1_Fixo = 0
            C1_Variavel = df_m1['Total'].sum()  # Tudo é variável
        
        C1_Total = C1_Fixo + C1_Variavel
        
        # 3. Obter volumes
        volume_m1 = df_vol_m1['Volume'].sum()
        volume_m2 = df_vol_m2['Volume'].sum()
        
        # 4. Calcular proporção
        rho = volume_m2 / volume_m1 if volume_m1 != 0 else 1.0
        
        # 5. Calcular Flex Mês 1 (em Custo Total)
        Flex_Mes1_Fixo = C1_Fixo  # Não varia
        Flex_Mes1_Variavel = C1_Variavel * rho  # Varia proporcionalmente
        Flex_Mes1_Total = Flex_Mes1_Fixo + Flex_Mes1_Variavel
        
        # 6. Converter para CPU (se necessário)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            BUD_CPU = C1_Total / volume_m1 if volume_m1 != 0 else 0
            Flex_Mes1_CPU = Flex_Mes1_Total / volume_m2 if volume_m2 != 0 else 0
            Delta_Flex = Flex_Mes1_CPU - BUD_CPU
        ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Exemplo Prático - Real x Real
        
        **Dados:**
        - Volume Real Mês 1 (`V_1`): 40,848 unidades
        - Volume Real Mês 2 (`V_2`): 60,333 unidades
        - Custo Total Fixo Mês 1 (`C_1_Fixo`): R$ 126.91
        - Custo Total Variável Mês 1 (`C_1_Variável`): R$ 755.36
        - Custo Total Mês 1 (`C_1_Total`): R$ 882.27
        
        **Cálculo:**
        ```
        rho = V_2 / V_1 = 60,333 / 40,848 = 1.482373
        
        Flex_Mês1_Fixo = R$ 126.91
        Flex_Mês1_Variável = R$ 755.36 * 1.482373 = R$ 1,119.72
        Flex_Mês1_Total = R$ 126.91 + R$ 1,119.72 = R$ 1,246.63
        ```
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 882.27 / 40,848 = R$ 0.0216 por unidade
        Flex_Mês1_CPU = R$ 1,246.63 / 60,333 = R$ 0.0207 por unidade
        Delta_Flex = R$ 0.0207 - R$ 0.0216 = -R$ 0.0009 por unidade
        ```
        
        **Interpretação:**
        - O volume aumentou 48.24% (`rho = 1.482373`)
        - O custo variável aumentou proporcionalmente: R$ 755.36 -> R$ 1,119.72
        - O custo fixo permaneceu igual: R$ 126.91
        - Em CPU, o custo por unidade diminuiu porque o custo fixo foi diluído em mais volume
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Modos de Comparação - Real x Real
        
        **Mês a Mês:**
        - `V_1` = Volume do mês inicial
        - `V_2` = Volume do mês final
        
        **Ano a Ano:**
        - `V_1` = Volume total do ano inicial
        - `V_2` = Volume total do ano final
        
        **Semestre:**
        - `V_1` = Volume total do semestre inicial
        - `V_2` = Volume total do semestre final
        
        **Quarter:**
        - `V_1` = Volume total do trimestre inicial
        - `V_2` = Volume total do trimestre final
    """)
    
    st.markdown("---")
    
    st.markdown("## 💰 CASO 2: Flex para Comparação Real x Budget (TC Ext)")
    
    st.markdown("""
        ### Contexto
        
        Usado na página **TC Ext** para comparar período real vs budget planejado:
        - **Real** = Dados reais do período
        - **Budget** = Dados planejados do período
        
        **Objetivo:** Calcular o que seria o budget ajustado pelo volume real.
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Regras de Cálculo - Real x Budget
        
        **Passo 1: Identificar Custos do Budget**
        ```python
        # Separar Fixo e Variável do Budget
        B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
        B_Variavel = df_budget[df_budget['Custo'] == 'Variável']['Total'].sum()
        B_Total = B_Fixo + B_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V_Budget = volume_budget  # Volume planejado no Budget
        V_Real = volume_real      # Volume real do período
        ```
        
        **Passo 3: Calcular Proporção de Volume**
        ```python
        rho = V_Real / V_Budget  # Proporção de volume real vs planejado
        ```
        
        **Passo 4: Aplicar Regras de Fixo e Variável**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo não varia com volume
        Flex_Bud_Fixo = B_Fixo
        # Explicação: Mantém o valor do budget, independente da variação de volume
        ```
        
        **Para Custo Variável:**
        ```python
        # REGRA: Variável varia proporcionalmente ao volume
        Flex_Bud_Variavel = B_Variavel * rho
                            = B_Variavel * (V_Real / V_Budget)
        # Explicação: Ajusta o budget variável pela proporção de volume real vs planejado
        ```
        
        **Passo 5: Calcular Flex Bud Total**
        ```python
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
                        = B_Fixo + (B_Variavel * rho)
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Fórmulas Matemáticas Completas - Real x Budget
        
        **Definições:**
        ```python
        # Separar Fixo e Variável do Budget
        B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
        B_Variavel = df_budget[df_budget['Custo'] == 'Variável']['Total'].sum()
        B_Total = B_Fixo + B_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V_Budget = volume_budget  # Volume planejado no Budget
        V_Real = volume_real      # Volume real do período
        ```
        
        **Passo 3: Calcular Proporção de Volume**
        ```python
        rho = V_Real / V_Budget  # Proporção de volume real vs planejado
        ```
        
        **Passo 4: Aplicar Regras de Fixo e Variável**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo não varia com volume
        Flex_Bud_Fixo = B_Fixo
        # Explicação: Mantém o valor do budget, independente da variação de volume
        ```
        **Fórmula Matemática:**
        ```
        Flex_Bud_Fixo = B_Fixo
        ```
        **Por que não multiplica pela proporção?**
        - Custos fixos são independentes do volume de produção
        - O budget fixo foi planejado e não deve ser ajustado
        - Exemplos: Aluguel, salários fixos, depreciação
        - Mesmo que o volume real seja diferente do planejado, o custo fixo permanece igual
        
        **Para Custo Variável:**
        ```python
        # REGRA: Variável varia proporcionalmente ao volume
        Flex_Bud_Variavel = B_Variavel * rho
                            = B_Variavel * (V_Real / V_Budget)
        # Explicação: Ajusta o budget variável pela proporção de volume real vs planejado
        ```
        **Fórmula Matemática:**
        ```
        Flex_Bud_Variável = B_Variável * rho
                           = B_Variável * (V_Real / V_Budget)
        ```
        **Por que multiplica pela proporção?**
        - Custos variáveis variam proporcionalmente ao volume
        - Se o volume real for maior que o planejado, o custo variável deve aumentar
        - Se o volume real for menor que o planejado, o custo variável deve diminuir
        - Exemplos: Matéria-prima, energia variável, comissões
        - O budget variável precisa ser ajustado para refletir o volume real
        
        **Passo 5: Calcular Flex Bud Total**
        ```python
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
                        = B_Fixo + (B_Variavel * rho)
        ```
        **Fórmula Matemática:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                       = B_Fixo + (B_Variável * rho)
                       = B_Fixo + B_Variável * (V_Real / V_Budget)
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Fórmulas Matemáticas Completas - Real x Budget
        
        **Definições:**
        - `V_Real` = Volume Real do período
        - `V_Budget` = Volume Budget planejado do período
        - `B_Fixo` = Custo Total Fixo do Budget
        - `B_Variável` = Custo Total Variável do Budget
        - `B_Total` = Custo Total do Budget = `B_Fixo + B_Variável`
        - `R_Total` = Custo Total Real do período
        
        **Proporção de Volume:**
        ```
        rho = V_Real / V_Budget
        ```
        Onde:
        - `rho > 1` significa que o volume real foi maior que o planejado
        - `rho < 1` significa que o volume real foi menor que o planejado
        - `rho = 1` significa que o volume real foi exatamente o planejado
        
        **Cálculo de Flex Bud (em Custo Total):**
        
        Para **Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
        ```
        **Regra Aplicada:** Fixo não varia com volume
        - Valor do budget mantido: `B_Fixo`
        - Não multiplica pela proporção de volume
        - Motivo: Custos fixos são independentes do volume, então mantém o valor planejado
        
        Para **Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável * rho
                           = B_Variável * (V_Real / V_Budget)
        ```
        **Regra Aplicada:** Variável varia proporcionalmente ao volume
        - Valor do budget: `B_Variável`
        - Multiplica pela proporção: `rho = V_Real / V_Budget`
        - Motivo: Se o volume real for maior que o planejado, o custo variável deve aumentar proporcionalmente
        
        **Flex Bud Total (em Custo Total):**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                       = B_Fixo + (B_Variável * rho)
                       = B_Fixo + B_Variável * (V_Real / V_Budget)
        ```
        **Regra Aplicada:** Soma do Fixo (inalterado) + Variável (ajustado)
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Cálculo em CPU (Custo por Unidade) - Real x Budget
        
        **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD em CPU:**
        ```
        BUD_CPU = B_Total / V_Budget
                 = (B_Fixo + B_Variável) / V_Budget
        ```
        
        **Flex Bud em CPU:**
        ```
        Flex_Bud_CPU = Flex_Bud_Total / V_Real
                     = [B_Fixo + B_Variável * (V_Real / V_Budget)] / V_Real
                     = (B_Fixo / V_Real) + (B_Variável / V_Budget)
        ```
        
        **Total Real em CPU:**
        ```
        Total_Real_CPU = R_Total / V_Real
        ```
        
        **Diferenças:**
        
        **Flex Bud - BUD:**
        ```
        Delta_Flex_Bud = Flex_Bud_CPU - BUD_CPU
                   = [(B_Fixo / V_Real) + (B_Variável / V_Budget)] - [(B_Fixo + B_Variável) / V_Budget]
                   = (B_Fixo / V_Real) - (B_Fixo / V_Budget)
                   = B_Fixo * (1/V_Real - 1/V_Budget)
                   = B_Fixo * (V_Budget - V_Real) / (V_Real * V_Budget)
        ```
        
        **Total - Flex Bud:**
        ```
        Delta_Total_Flex = Total_Real_CPU - Flex_Bud_CPU
                     = (R_Total / V_Real) - [(B_Fixo / V_Real) + (B_Variável / V_Budget)]
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Implementação - Real x Budget
        
        ```python
        # 1. Obter dados de Budget
        df_budget = load_budget_data(ano_selecionado)
        
        # 2. Separar Fixo e Variável do Budget
        if 'Custo' in df_budget.columns:
            B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
            B_Variavel = df_budget[df_budget['Custo'] == 'Variável']['Total'].sum()
        else:
            B_Fixo = 0
            B_Variavel = df_budget['Total'].sum()  # Tudo é variável
        
        B_Total = B_Fixo + B_Variavel
        
        # 3. Obter volumes
        volume_real = df_vol_real['Volume'].sum()
        volume_budget = df_vol_budget['Volume'].sum()
        
        # 4. Calcular proporção
        rho = volume_real / volume_budget if volume_budget != 0 else 1.0
        
        # 5. Calcular Flex Bud (em Custo Total)
        Flex_Bud_Fixo = B_Fixo  # Não varia
        Flex_Bud_Variavel = B_Variavel * rho  # Varia proporcionalmente
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
        
        # 6. Converter para CPU (se necessário)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            BUD_CPU = B_Total / volume_budget if volume_budget != 0 else 0
            Flex_Bud_CPU = Flex_Bud_Total / volume_real if volume_real != 0 else 0
            Total_Real_CPU = df_real['Total'].sum() / volume_real if volume_real != 0 else 0
            
            Delta_Flex_Bud = Flex_Bud_CPU - BUD_CPU
            Delta_Total_Flex = Total_Real_CPU - Flex_Bud_CPU
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Exemplo Prático - Real x Budget
        
        **Dados:**
        - Volume Real (`V_Real`): 50,000 unidades
        - Volume Budget (`V_Budget`): 60,000 unidades
        - Custo Total Fixo Budget (`B_Fixo`): R$ 200,000
        - Custo Total Variável Budget (`B_Variável`): R$ 400,000
        - Custo Total Budget (`B_Total`): R$ 600,000
        - Custo Total Real (`R_Total`): R$ 550,000
        
        **Cálculo Passo a Passo:**
        
        **1. Calcular Proporção de Volume:**
        ```
        rho = V_Real / V_Budget = 50,000 / 60,000 = 0.833333
        ```
        *Interpretação: Volume real foi 16.67% menor que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo não varia -> mantém valor do budget*
        
        **3. Aplicar Regra para Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável * rho
                           = R$ 400,000 * 0.833333
                           = R$ 333,333.33
        ```
        *Regra: Variável varia proporcionalmente -> ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                        = R$ 200,000 + R$ 333,333.33
                        = R$ 533,333.33
        ```
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 600,000 / 60,000 = R$ 10.00 por unidade
        Flex_Bud_CPU = R$ 533,333.33 / 50,000 = R$ 10.67 por unidade
        Total_Real_CPU = R$ 550,000 / 50,000 = R$ 11.00 por unidade
        
        Delta_Flex_Bud = R$ 10.67 - R$ 10.00 = R$ 0.67 por unidade
        Delta_Total_Flex = R$ 11.00 - R$ 10.67 = R$ 0.33 por unidade
        ```
        
        **Interpretação:**
        - O volume real foi 16.67% menor que o planejado (`rho = 0.833333`)
        - O budget variável foi ajustado proporcionalmente: R$ 400,000 -> R$ 333,333.33
        - O budget fixo permaneceu igual: R$ 200,000
        - Em CPU, o Flex Bud aumentou porque o custo fixo foi concentrado em menos volume
        - O Total Real está R$ 0.33 acima do Flex Bud, indicando ineficiência operacional
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Exemplo Prático 2 - Real x Budget (Volume Real > Volume Budget)
        
        **Dados:**
        - Volume Real (`V_Real`): 62,208 unidades
        - Volume Budget (`V_Budget`): 60,120 unidades
        - Custo Total Fixo Budget (`B_Fixo`): R$ 200,000
        - Custo Total Variável Budget (`B_Variável`): R$ 400,000
        - Custo Total Budget (`B_Total`): R$ 600,000
        
        **Cálculo Passo a Passo:**
        
        **1. Calcular Proporção de Volume:**
        ```
        rho = V_Real / V_Budget = 62,208 / 60,120 = 1.0347
        ```
        *Interpretação: Volume real foi 3.47% maior que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo não varia -> mantém valor do budget*
        
        **3. Aplicar Regra para Custo Variável:**
        ```
        Flex_Bud_Variável = B_Variável * rho
                           = R$ 400,000 * 1.0347
                           = R$ 413,880
        ```
        *Regra: Variável varia proporcionalmente -> ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variável
                        = R$ 200,000 + R$ 413,880
                        = R$ 613,880
        ```
        *Resultado: Flex_Bud_Total (R$ 613,880) > BUD_Total (R$ 600,000) [CORRETO]*
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 600,000 / 60,120 = R$ 9.98 por unidade
        Flex_Bud_CPU = R$ 613,880 / 62,208 = R$ 9.87 por unidade
        ```
        
        **Diferenças:**
        ```
        Delta_Flex_Bud (Custo Total) = R$ 613,880 - R$ 600,000 = R$ 13,880 (positivo) [CORRETO]
        Delta_Flex_Bud (CPU) = R$ 9.87 - R$ 9.98 = -R$ 0.11 (negativo)
        ```
        
        **Interpretação:**
        - O volume real foi 3.47% maior que o planejado (`rho = 1.0347`)
        - O budget variável foi ajustado proporcionalmente: R$ 400,000 -> R$ 413,880
        - O budget fixo permaneceu igual: R$ 200,000
        - **Em Custo Total:** Flex_Bud_Total > BUD_Total (porque o custo variável aumentou)
        - **Em CPU:** Flex_Bud_CPU < BUD_CPU (porque o custo fixo foi diluído em mais volume)
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Comparação: Real x Real vs Real x Budget
        
        | Aspecto | Real x Real (Waterfall) | Real x Budget (TC Ext) |
        |---------|------------------------|------------------------|
        | **Base** | Custo Real Mês 1 | Custo Budget |
        | **Volume Referência** | Volume Real Mês 1 | Volume Budget |
        | **Volume Ajuste** | Volume Real Mês 2 | Volume Real |
        | **Proporção** | `V_2 / V_1` | `V_Real / V_Budget` |
        | **Objetivo** | Ajustar Mês 1 pelo volume do Mês 2 | Ajustar Budget pelo volume Real |
        | **Uso** | Comparar dois períodos reais | Comparar Real vs Planejado |
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Regras Gerais Aplicáveis a Ambos os Casos
        
        **1. Custo Fixo:**
        - Sempre mantém o valor original (não varia com volume)
        - `Flex_Fixo = Valor_Original`
        
        **2. Custo Variável:**
        - Varia proporcionalmente ao volume
        - `Flex_Variável = Valor_Original * (Volume_Novo / Volume_Original)`
        
        **3. Ordem de Cálculo:**
        1. Calcular em **Custo Total** primeiro
        2. Separar Fixo e Variável
        3. Aplicar proporção de volume apenas ao Variável
        4. Somar Fixo + Variável ajustado
        5. Se necessário, converter para **CPU** dividindo pelo volume final
        
        **4. Tratamento de Divisão por Zero:**
        - Se `Volume_Original = 0`: usar `rho = 1.0` (sem ajuste)
        - Se `Volume_Final = 0`: usar `Flex_CPU = 0`
        """)

# ==========================================
# TC VEÍCULOS: CÁLCULO POR TABELAS/GRÁFICOS
# ==========================================
elif indice_selecionado == "🧮 Cálculo por Tabelas/Gráficos (Normal vs CPU)" and modulo_doc == "🚗 TC Veículos":
    st.header("🧮 Cálculo por Tabelas/Gráficos — TC Veículos")

    st.info(
        "📌 **Módulo TC Veículos** — Tabelas e gráficos específicos do TC Veículos."
    )

    with st.expander("📊 **Análise Flex por Categoria**", expanded=True):
        st.markdown("""
        ### 🔍 Modos de Visualização

        - **Fixo/Variável**: Expanders `💰 Fixo` e `💰 Variável`, cada um com sub-expanders por `Type 05` → tabela por `Account`
        - **Total**: Expanders direto por `Type 05` → tabela por `Account`

        **Expander TOTAL:**
        - Re-agrega **todas** as linhas das oficinas por `(Type 05, Type 06, Account, Custo)`
        - Mostra tabela detalhada com todas as contas (não apenas 1 linha sintética)
        - Mesmo layout dos expanders por oficina

        ### 📋 Tabela Flex por Account

        | Coluna | Cálculo |
        |--------|---------|
        | Account | Nome da conta |
        | BUD | Σ Custo FP Budget |
        | Flex Bud − BUD | Flex − BUD |
        | Flex BUD | Fixo: BUD / Variável: BUD × Proporção |
        | Total − Flex Bud | Real − Flex |
        | Total | Σ Custo FP Real |
        | Total / Flex Bud | Real/Flex (com barrinha de progresso) |

        ### 🎨 Barrinha de Progresso
        - 🟢 Verde: ≤ 90%
        - 🟡 Gradiente verde→vermelho: 90%–100%
        - 🔴 Vermelho: ≥ 100%
        """)

    with st.expander("📈 **Gráficos do TC Veículos**", expanded=False):
        st.markdown("""
        ### 📊 Custo FP por Período
        - **Barras**: Real por período com degradê roxo (`scheme='purples'`)
        - **Linha pontilhada**: Flex BUD (laranja, `strokeDash=[10,5]`)
        - **Delta**: Gráfico inferior com `Real − Flex BUD` (verde/vermelho)
        - Biblioteca: **Altair** com `data_transformers.disable_max_rows()`

        ### 🎨 Cores do Best Estimate
        Na página de **Análise BE**, os gráficos por período usam codificação por cor
        na coluna `Tipo` para diferenciar meses:
        - 🟣 **Roxo escuro** (`#4C1D95`): meses **Históricos** (realizados)
        - 🟣 **Roxo claro** (`#C4B5FD`): meses de **Best Estimate** (projetados)

        ### 📊 Volume
        - **Barras**: Volume Budget (degradê verde)
        - **Linha tracejada**: Volume Realizado (laranja)
        - **Por Veículo**: Barras agrupadas por modelo

        ### 📊 Custos por Oficina
        - Barras Custo FP por Oficina
        - Barras Rateio FA por Oficina (verde/vermelho)
        - Tabela BUD vs Flex pivotada Oficina × Período
        """)

    with st.expander("📋 **Tabs Disponíveis**", expanded=False):
        st.markdown("""
        ### 🗂️ Organização em Tabs

        O TC Veículos organiza os dados em **6 tabs**:

        | Tab | Conteúdo |
        |-----|----------|
        | 🚗 TC Veículos | KPIs resumo + Gráfico Custo FP × Flex BUD por período |
        | 📊 Análise Flex | Fixo/Variável com hierarquia Type 05 → Account |
        | 📈 Volume | Budget vs Realizado (por período e por veículo) |
        | 🏢 Custos por Oficina | Custo FP e Rateio FA por oficina |
        | ⏱️ Tempo de Produção | Tempo Veículo vs Tempo FA por oficina |
        | 📋 Dados Detalhados | Tabelas exportáveis de Real e Budget |
        """)

# ==========================================
# SEÇÃO 2: CÁLCULO POR TABELAS/GRÁFICOS
# ==========================================
elif indice_selecionado == "🧮 Cálculo por Tabelas/Gráficos (Normal vs CPU)":
    st.header("🧮 Cálculo por Tabelas/Gráficos — TC Extendido")

    st.info(
        "Consulta rápida para evitar divergências entre gráfico e tabela. "
        "A referência completa está em `DOCUMENTACAO_SISTEMA_TC.md` (aba 🧾 Especificação Técnica)."
    )
    st.button(
        "➡️ Abrir a Especificação Técnica completa",
        key="btn_ir_especificacao_calculos_por_visualizacao",
        use_container_width=True,
        on_click=_ir_para_especificacao_tecnica,
    )

    caminho_doc = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    if not os.path.exists(caminho_doc):
        st.error(f"Arquivo não encontrado: {caminho_doc}")
    else:
        try:
            with open(caminho_doc, "r", encoding="utf-8") as f:
                conteudo = f.read()

            def _extrair_trecho(md: str) -> str:
                start_token = "### 9.6 Guia de cálculo por visualização"
                start = md.find(start_token)
                if start == -1:
                    start_token = "## 9) Gráficos e tabelas"
                    start = md.find(start_token)
                if start == -1:
                    return "⚠️ Não encontrei a seção de cálculos no arquivo de especificação."

                end = md.find("\n## ", start + 1)
                if end == -1:
                    end = len(md)
                return md[start:end].strip()

            st.markdown("---")
            st.markdown(_extrair_trecho(conteudo))
        except Exception as e:
            st.error(f"Erro ao carregar/parsear especificação: {e}")

# ==========================================
# TC VEÍCULOS: ARQUITETURA E ESTRUTURA
# ==========================================
elif indice_selecionado == "🏗️ Arquitetura e Estrutura" and modulo_doc == "🚗 TC Veículos":
    st.header("🏗️ Arquitetura e Estrutura — TC Veículos")

    st.info(
        "📌 **Módulo TC Veículos** — Estrutura de pastas, contratos de dados e pipeline de processamento."
    )

    with st.expander("📁 **Contratos de Dados (Parquets)**", expanded=True):
        st.markdown("""
        ### 📂 Estrutura de Pastas

        ```
        dados/TC_Principal/
        ├── {ano}/
        │   ├── BUD/
        │   │   ├── df_principal_BUD.parquet         # Custo consolidado BUD
        │   │   ├── df_vol_veiculos_BUD.parquet      # Volume por veículo BUD
        │   │   ├── df_veiculos_custo_fp_BUD.parquet  # Custo FP rateado BUD
        │   │   ├── df_veiculos_cpu_BUD.parquet      # CPU por veículo BUD
        │   │   ├── df_tempo_veiculos_BUD.parquet    # Tempo de produção BUD
        │   │   ├── df_dea_dedicado_BUD.parquet      # D&A Dedicado BUD
        │   │   └── df_volume_fa_BUD.parquet         # Volume Fluxo Anexo BUD
        │   ├── df_principal.parquet                 # Custo Real consolidado
        │   ├── df_vol_veiculos_actual.parquet       # Volume Realizado
        │   ├── df_veiculos_custo_fp.parquet         # Custo FP Real rateado
        │   └── df_veiculos_cpu.parquet              # CPU Real
        ├── Forecast/
        │   ├── forecast_completo.parquet            # Projeção BE mês a mês
        │   └── premissas.json                       # Premissas do simulador
        └── historico_consolidado/
            ├── df_principal_historico.parquet        # Multi-ano consolidado
            └── BUD/
                └── df_principal_historico_BUD.parquet
        ```

        ### 📋 Schema — Principal BUD

        | Coluna | Tipo | Descrição |
        |--------|------|-----------|
        | `Oficina` | str | Centro de custo (oficina) |
        | `Veículo` | str | Modelo do veículo |
        | `Type 05` | str | Classificação nível 1 |
        | `Type 06` | str | Classificação nível 2 |
        | `Custo` | str | Fixo ou Variável |
        | `Account` | str | Conta contábil (inclui "Redis") |
        | `Período` | str | Mês por extenso |
        | `Despesa Primaria` | float | Despesa primária (R$) |
        | `Custo FA` | float | Custo do Fluxo Anexo |
        | `Custo FP` | float | Custo FP consolidado |
        | `D&A dedicado` | float | D&A dedicada |
        | `FP sem Dedicada` | float | Custo FP sem D&A |

        ### 📋 Schema — Veículos Rateado (BUD)

        | Coluna | Tipo | Descrição |
        |--------|------|-----------|
        | `Oficina` | str | Centro de custo |
        | `Veículo` | str | Modelo do veículo |
        | `Custo Rateado` | float | Custo × percentual do veículo |
        | `D&A dedicado` | float | D&A dedicada direta |
        | `Custo FP Veiculo` | float | Rateado + D&A |
        | `Ano` | int | Ano de referência |

        > O parquet BUD veículos tem `Custo FP Veiculo` (não `Custo FP`). O sistema faz mapeamento automático.
        """)

    with st.expander("🔧 **Módulos e Arquivos**", expanded=False):
        st.markdown("""
        ### 📂 Estrutura do Código

        ```
        tc_principal/
        ├── __init__.py
        ├── shared.py              # Constantes, loaders, helpers, ratear_be_por_veiculo()
        ├── ui_components.py       # Sidebar filters, CSS, KPIs
        └── pages/
            ├── __init__.py
            ├── home_tc.py                      # Página principal (6 tabs)
            ├── best_estimate_simulador_tc.py   # Simulador de premissas BE
            ├── best_estimate_analise_tc.py     # Dashboard de análise BE
            └── waterfall_tc.py                 # Análise Waterfall (Real + Budget)
        ```

        ### ⚙️ Filtros — Arquitetura Unificada

        ```
        Sidebar filters
             │
             ├── Veículo = "Todos" ──► usar_rateado = False
             │         ├── df_principal_BUD  → df_bud
             │         └── df_principal_Real → df
             │
             └── Veículo = "CC21 biton" ──► usar_rateado = True
                       ├── df_veiculos_custo_fp_BUD → df_bud (filtrado)
                       └── df_veiculos_custo_fp_Real → df (filtrado)
             │
        aplicar_fator_df() + converter_moeda_df()
             │
        calcular_flex_budget()
             │
        ┌─────────────────────────────┐
        │  Todos os tabs usam         │
        │  df_bud, df, df_vol_bud,    │
        │  df_vol_actual, df_flex     │
        └─────────────────────────────┘
        ```
        """)

    with st.expander("⚙️ **ETL e Processamento**", expanded=False):
        st.markdown("""
        ### 📋 Arquivos de Processamento

        | Arquivo | Função |
        |---------|--------|
        | `processamento_dados_BUD.py` | Processa dados Budget (principal + veículos) |
        | `processamento_dados_veiculos_BUD.py` | Rateio por veículo + CPU |
        | `processamento_dados.py` | Processa dados Real (Sapiens) |

        ### 🔄 Pipeline

        1. Extração dos dados brutos (Excel/SAP)
        2. Normalização de colunas e períodos
        3. Cálculo de composição de custos (Desp. Primária → FA → FP)
        4. Rateio por veículo (tempo de produção)
        5. Cálculo de CPU por veículo
        6. Gravação em Parquet na pasta `dados/TC_Principal/{ano}/`

        ### 💾 Cache
        - `@st.cache_data(ttl=3600)` em todos os loaders
        - Botão "🔄 Limpar Cache" na sidebar para forçar recarga
        """)

    with st.expander("🌐 **Configurações Globais**", expanded=False):
        st.markdown("""
        ### 💱 Moeda

        | Código | Símbolo | Conversão |
        |--------|---------|-----------|
        | BRL | R$ | 1.0 (base) |
        | USD | $ | 1/Taxa USD→BRL |
        | EUR | € | 1/Taxa EUR→BRL |

        ### 📊 Fator

        | Opção | Divisor |
        |-------|---------|
        | Nenhum | 1 |
        | K (milhares) | 1.000 |
        | M (milhões) | 1.000.000 |

        ### 👁️ Tipo de Visualização

        | Tipo | Comportamento |
        |------|---------------|
        | Custo Total | Valores absolutos em R$/USD/EUR |
        | CPU | Custo ÷ Volume (fator = Nenhum) |
        """)

# ==========================================
# SEÇÃO 2: ARQUITETURA E ESTRUTURA
# ==========================================
elif indice_selecionado == "🏗️ Arquitetura e Estrutura":
    st.header("🏗️ Arquitetura e Estrutura — TC Extendido")

    st.warning(
        "⚠️ **Seção legada/complementar:** a arquitetura e contratos canônicos (estrutura do projeto, "
        "pipelines, schemas e critérios de aceite) estão em `DOCUMENTACAO_SISTEMA_TC.md` na seção "
        "**🧾 Especificação Técnica**."
    )
    st.button(
        "➡️ Ir para a Especificação Técnica",
        key="btn_ir_especificacao_arquitetura",
        use_container_width=True,
        on_click=_ir_para_especificacao_tecnica,
    )
    
    st.markdown("""
    Esta seção documenta a arquitetura, estrutura de arquivos, tecnologias utilizadas
    e informações sobre a equipe responsável pelo desenvolvimento do projeto.
    """)
    
    st.markdown("---")
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("💻 Linhas de Código", "20.000+", "Sistema completo")
    
    with col2:
        st.metric("📊 Páginas", "6", "Funcionalidades completas")
    
    with col3:
        st.metric("⚡ Otimização", "70%+", "Memória reduzida")
    
    with col4:
        st.metric("📁 Arquivos", "Parquet", "Formato otimizado")
        
    # EXPANDER 1: Estrutura de Arquivos
    with st.expander("📁 **Estrutura de Arquivos e Organização do Projeto**", expanded=False):
        st.subheader("📁 Estrutura de Arquivos")
        
        st.markdown("""
        ### Estrutura do Projeto
        
        ```
        C:\\GIT\\TC\\
        ├── app.py                                    # Portal / Router (menu via st.navigation)
        ├── pages\\
        │   ├── 1 - Waterfall.py                     # Análise waterfall (~4.000 linhas)
        │   ├── 2 - Best Estimate - Simulador.py     # Simulador de Best Estimate (~4.300 linhas)
        │   ├── (removido) 3 - Best Estimate - Análise.py  # Análise legacy (substituída pela BE (Análise) baseada na Home)
        │   ├── (removido) Waterfall_Analysis.py     # Página duplicada removida
        │   ├── 5 - Extração de Dados.py             # Extração e processamento de dados (~600 linhas)
        │   └── 6 - Documentacao.py                  # Documentação (este arquivo) (~3.900 linhas)
        ├── tc_ext\\
        │   └── pages\\
        │       ├── home_ext.py                      # Home (TC Ext)
        │       └── be_analise_ext.py                # Best Estimate (Análise) (base Home; lê dados/Forecast)
        ├── dados\\
        │   ├── historico_consolidado\\
        │   │   ├── df_final_historico.parquet
        │   │   ├── df_ke5z_historico.parquet
        │   │   ├── df_vol_historico.parquet
        │   │   └── BUD\\
        │   │       ├── df_final_historico_BUD.parquet
        │   │       ├── df_ke5z_historico_BUD.parquet
        │   │       └── df_vol_historico_BUD.parquet
        │   ├── 2024\\
        │   │   ├── df_final.parquet
        │   │   ├── df_vol.parquet
        │   │   └── ... (outros arquivos)
        │   ├── 2025\\
        │   │   ├── df_final.parquet
        │   │   ├── df_vol.parquet
        │   │   └── BUD\\
        │   │       ├── df_final_BUD.parquet
        │   │       └── df_vol_BUD.parquet
        │   └── Forecast\\
        │       ├── df_final_historico_forecast.parquet
        │       ├── df_vol_historico.parquet
        │       ├── forecast_completo.parquet
        │       ├── forecast_historico.parquet
        │       └── forecast_previsao.parquet
        └── tc_ext/notebooks/dados.ipynb                               # Notebook para processar dados
        ```
        
        **Observações:**
        - Arquivos principais: `historico_consolidado/` (usado pelo sistema)
        - Dados por ano: `2024/` e `2025/` (para filtros específicos)
        - Forecast: `Forecast/` (dados processados para previsões)
        - Formato: Parquet para performance otimizada
        """)

        st.markdown("---")
        
        st.subheader("📄 Arquivos Principais")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **app.py** (~10.000 linhas)
            - Dashboard principal TC Ext
            - Análise de custos com comparação Budget
            - Cálculo Flex Bud
            - Gráficos interativos
            - Tabelas hierárquicas
            - Exportação Excel
            
            **pages/1 - Waterfall.py** (~4.000 linhas)
            - Análise waterfall entre períodos
            - Cálculo Flex Mês 1
            - Gráficos waterfall interativos
            - Tabelas com hierarquia
            
            **pages/2 - Best Estimate - Simulador.py** (~4.300 linhas)
            - Simulação interativa de Best Estimate
            - Ajuste de sensibilidade em tempo real
            - Configuração de inflação
            - Gráficos de premissas
            """)
        
        with col2:
            st.markdown("""
            **tc_ext/pages/be_analise_ext.py**
            - Best Estimate (Análise) no TC Ext (substitui a análise legacy)
            - Mesma base visual e de cálculo da Home (TC Ext)
            - Lê os outputs do simulador em `dados/Forecast/`
            - Regra de CPU aplicada de forma consistente (Total/Volume)
            
            **(removido) pages/4 - Waterfall_Analysis.py** (página duplicada removida)
            - Análise waterfall entre períodos (legado)
            - Cálculo Flex Mês 1
            - Gráficos waterfall interativos
            
            **pages/5 - Extração de Dados.py** (~600 linhas)
            - Interface para extração e processamento de dados
            - Upload de arquivos
            - Validação de arquivos
            - Execução de notebooks de processamento
            
            **pages/6 - Documentacao.py** (~3.900 linhas)
            - Documentação completa do sistema
            - Regras e cálculos
            - Arquitetura e estrutura
            - Guia de extração de dados
            """)
    
    # Sub-expander: Estrutura da Pasta dados
    with st.expander("📂 **Estrutura e Funcionamento da Pasta `dados/`**", expanded=False):
        st.markdown("""
            ### 📂 Organização da Pasta `dados/`
            
            A pasta `dados/` é o coração do sistema, onde todos os arquivos processados são armazenados.
            Ela é organizada de forma hierárquica para facilitar o gerenciamento e acesso aos dados.
            
            **Estrutura Completa:**
            ```
            dados/
            ├── historico_consolidado/          # 📚 Dados históricos consolidados (PRINCIPAL)
            │   ├── df_final_historico.parquet  # Dados de custos históricos consolidados
            │   ├── df_ke5z_historico.parquet  # Dados KE5Z históricos consolidados
            │   ├── df_vol_historico.parquet    # Volumes históricos consolidados
            │   └── BUD/                        # 📊 Dados de Budget históricos
            │       ├── df_final_historico_BUD.parquet
            │       ├── df_ke5z_historico_BUD.parquet
            │       └── df_vol_historico_BUD.parquet
            │
            ├── 2024/                           # 📅 Dados específicos do ano 2024
            │   ├── df_final.parquet           # Dados de custos do ano
            │   ├── df_vol.parquet             # Volumes do ano
            │   ├── df_ke5z_group.parquet      # Dados KE5Z agrupados
            │   └── Dados SAPIENS.xlsx          # Arquivo fonte (entrada)
            │   └── Reporting fluxo anexo.xlsx  # Arquivo fonte (entrada)
            │
            ├── 2025/                           # 📅 Dados específicos do ano 2025
            │   ├── df_final.parquet
            │   ├── df_vol.parquet
            │   ├── df_ke5z_group.parquet
            │   ├── BUD/                        # 📊 Budget do ano 2025
            │   │   ├── df_final_BUD.parquet
            │   │   └── df_vol_BUD.parquet
            │   └── ... (arquivos fonte)
            │
            └── Forecast/                       # 🔮 Dados processados para Forecast
                ├── df_final_historico_forecast.parquet
                ├── df_vol_historico.parquet
                ├── forecast_completo.parquet
                ├── forecast_historico.parquet
                └── forecast_previsao.parquet
            ```
            """)
            
        st.markdown("---")
            
        st.markdown("""
            ### 🔄 Como as Pastas São Criadas e Atualizadas
            
            **1. Criação Inicial da Estrutura:**
            
            Quando o sistema é executado pela primeira vez ou quando novos dados são processados,
            o sistema verifica e cria automaticamente as pastas necessárias:
            
            ```python
            # Exemplo de criação de pastas (tc_ext/notebooks/dados.ipynb)
            PASTA_ANO = f'dados/{ANO_ATUAL}'  # Ex: dados/2025
            PASTA_HISTORICO = 'dados/historico_consolidado'
            PASTA_BUD = f'dados/{ANO_ATUAL}/BUD'
            
            # Criar estrutura de pastas
            os.makedirs(PASTA_ANO, exist_ok=True)
            os.makedirs(PASTA_HISTORICO, exist_ok=True)
            os.makedirs(PASTA_BUD, exist_ok=True)
            ```
            
            **2. Processo de Atualização:**
            
            **a) Processamento de Dados do Ano:**
            - Os arquivos Excel (`Dados SAPIENS.xlsx`, `Reporting fluxo anexo.xlsx`) são colocados na pasta do ano (ex: `dados/2025/`)
            - O notebook `tc_ext/notebooks/dados.ipynb` processa esses arquivos e gera os arquivos Parquet
            - Os arquivos Parquet são salvos na mesma pasta do ano
            - **Simultaneamente**, os dados são consolidados no histórico
            
            **b) Consolidação no Histórico:**
            - Após processar os dados do ano, o sistema **concatena** os novos dados com o histórico existente
            - Os arquivos em `historico_consolidado/` são **atualizados** (não substituídos)
            - Isso permite que o sistema tenha acesso a **todos os dados históricos** em um único lugar
            
            **c) Processamento de Budget:**
            - Similar ao processo de dados do ano, mas os arquivos são processados pelo `tc_ext/notebooks/dados_BUD.ipynb`
            - Os **outputs** de Budget (parquets/diagnósticos) são salvos em `dados/{ANO}/BUD/`
            - Os **inputs** (Excels) são os mesmos do Real e ficam em `dados/{ANO}/`
            - O histórico de Budget é consolidado em `historico_consolidado/BUD/`
            
            **d) Processamento de Forecast:**
            - Quando o Forecast é gerado (páginas 2 ou 3 - Best Estimate), a pasta `dados/Forecast/` é criada automaticamente
            - Os arquivos de forecast são salvos diretamente nesta pasta
            - A pasta é criada dinamicamente se não existir:
            ```python
            pasta_dados = "dados"
            pasta_forecast = os.path.join(pasta_dados, "Forecast")
            
            if not os.path.exists(pasta_forecast):
                os.makedirs(pasta_forecast, exist_ok=True)
            ```
            """)
        
        st.markdown("---")
        
        st.markdown("""
        ### 🔗 Como as Pastas Funcionam Entre Si
            
            **1. Relação entre Pastas por Ano e Histórico:**
            
            ```
            dados/2025/df_final.parquet  ──┐
                                           ├──> Concatena ──> dados/historico_consolidado/df_final_historico.parquet
            dados/2024/df_final.parquet  ──┘
            ```
            
            - **Dados do Ano:** Contêm apenas os dados do ano específico (útil para filtros rápidos)
            - **Histórico Consolidado:** Contém **TODOS** os anos concatenados (usado pelo sistema principal)
            - O sistema **prioriza** o histórico consolidado para análises que precisam de múltiplos anos
            
            **2. Fluxo de Dados:**
            
            ```
            Arquivos Excel (entrada)
                │
                ├──> Processamento (tc_ext/notebooks/dados.ipynb)
                │       │
                │       ├──> Salva em dados/{ANO}/ (dados do ano)
                │       │
                │       └──> Concatena em historico_consolidado/ (histórico completo)
                │
                └──> Sistema Streamlit lê de historico_consolidado/ (fonte principal)
            ```
            
            **3. Separação de Budget:**
            
            - **Dados Reais:** `dados/{ANO}/` e `historico_consolidado/`
            - **Dados Budget (outputs):** `dados/{ANO}/BUD/` e `historico_consolidado/BUD/`
            - Esta separação evita misturar outputs de Budget com Real
            
            **4. Forecast como Dados Derivados:**
            
            - A pasta `Forecast/` contém dados **processados e calculados** pelo sistema
            - Não são dados de entrada, mas sim **resultados** de cálculos de forecast
            - São gerados dinamicamente quando o usuário executa o Forecast
            """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ⚙️ Regras de Criação e Atualização
            
            **Regra 1: Criação Automática**
            - Todas as pastas são criadas automaticamente quando necessário
            - O parâmetro `exist_ok=True` garante que não há erro se a pasta já existir
            - Não é necessário criar manualmente nenhuma pasta
            
            **Regra 2: Consolidação Incremental**
            - O histórico é **atualizado** (não substituído) a cada processamento
            - Novos dados são **adicionados** ao histórico existente
            - Isso mantém a integridade dos dados históricos
            
            **Regra 3: Separação por Tipo**
            - Dados Reais e Budget são mantidos **separados** em pastas diferentes
            - Isso evita confusão e permite comparações precisas
            - O sistema sabe qual pasta usar baseado no modo de comparação selecionado
            
            **Regra 4: Formato Parquet**
            - Todos os arquivos processados são salvos em formato **Parquet**
            - Parquet oferece compressão e leitura rápida
            - Formato otimizado para grandes volumes de dados
            """)
    
    # EXPANDER 2: Tecnologias
    with st.expander("💻 **Tecnologias e Bibliotecas**", expanded=False):
        st.subheader("💻 Tecnologias e Bibliotecas")
        
        st.markdown(f"""
        ### Stack Tecnológico
        
        **Framework Principal:**
        - **Streamlit** {st.__version__} - Framework web para aplicações de dados
        
        **Linguagem:**
        - **Python** 3.8+ - Linguagem de programação
        
        **Processamento de Dados:**
        - **Pandas** 2.0.0+ - Manipulação e análise de dados
        - **NumPy** 1.24.0+ - Operações numéricas
        
        **Visualizações:**
        - **Altair** 5.0.0+ - Gráficos interativos
        - **Plotly** - Gráficos waterfall avançados
        
        **Formato de Dados:**
        - **PyArrow** 12.0.0+ - Suporte a Parquet
        - **Parquet** - Formato de dados otimizado
        
        **Exportação:**
        - **OpenPyXL** 3.1.0+ - Geração de arquivos Excel
        """)
        
        st.markdown("---")
        
        st.subheader("🔧 Dependências Principais")
        
        st.code("""
# requirements.txt
streamlit>=1.28.0
pandas>=2.0.0
altair>=5.0.0
numpy>=1.24.0
openpyxl>=3.1.0
pyarrow>=12.0.0
plotly>=5.0.0
        """, language="text")
        
        st.markdown("---")
        
        st.subheader("⚡ Otimizações Implementadas")
        
        st.markdown("""
        **Gestão de Memória:**
        - Cache inteligente com TTL configurável
        - Otimização de tipos: Category para strings repetidas
        - Downcast: Float64 -> Float32, Int64 -> Int32
        - Redução de cópias: Apenas quando necessário
        
        **Operações Vetorizadas:**
        - Substituição de `iterrows()` por merge e `np.where()`
        - Substituição de `apply()` por operações vetorizadas
        - Filtros booleanos ao invés de loops
        - Agrupamento otimizado com `agg()` direto
        
        **Cálculos Otimizados:**
        - CPU calculado após agrupamento (nunca antes)
        - Flex Bud com merge ao invés de loops
        - Volume sincronizado entre tabelas e gráficos
        - Cache de filtros para opções repetidas
        """)
    
    # EXPANDER 3: Desafios e Soluções
    with st.expander("⚠️ **Desafios Principais & Soluções Implementadas**", expanded=False):
        st.markdown("""
        ### 📊 Desafios Identificados
        
        - **📁 Dados grandes:** Milhões de registros causando lentidão
        - **💾 Uso de memória:** Excedia limites de processamento
        - **Instabilidade:** Sistema lento com muitos filtros
        - **🐌 Cálculos complexos:** Flex Bud e Forecast demorados
        - **🔄 Sincronização:** Dados de tabela vs gráficos diferentes
        - **📊 Visualizações:** Gráficos sem gradientes e pouco informativos
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ✅ Soluções Implementadas
        
        - **📊 Otimização de dados:** Parquet com tipos categóricos
        - **⚡ Cache estratégico:** TTL configurável por tipo de dado
        - **🔄 Operações vetorizadas:** Substituição de iterrows() e apply()
        - **📈 Cálculos otimizados:** Flex Bud e CPU após agrupamento
        - **🎯 Sincronização:** Mesma fonte de dados para tabelas e gráficos
        - **🎨 Visualizações melhoradas:** Gradientes, delta charts, barras HTML
        """)
        
        st.info("🎆 **Resultado Final:** Sistema 100% estável com performance otimizada e visualizações profissionais!")
    
    # EXPANDER 4: Estatísticas do Sistema
    with st.expander("📊 **Estatísticas e Métricas do Sistema**", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 💾 Dados e Performance
            
            **📁 Arquivos Principais:**
            - `df_final_historico.parquet` (dados históricos)
            - `df_vol_historico.parquet` (volumes)
            - `df_final_historico_BUD.parquet` (budget)
            
            **⚡ Otimizações:**
            - Tipos categóricos para strings
            - Downcast de numéricos
            - Compressão Parquet
            - Cache com TTL
            """)
        
        with col2:
            st.markdown("""
            ### 📊 Páginas do Sistema
            
            **📄 Páginas Disponíveis:**
            - `app.py` - Portal / Router (menu via st.navigation)
            - `1 - Waterfall.py` - Análise waterfall (~4.000 linhas)
            - `2 - Best Estimate - Simulador.py` - Simulação (~4.300 linhas)
            - `tc_ext/pages/be_analise_ext.py` - Best Estimate (Análise) (base Home)
            - `4 - Waterfall_Analysis.py` - (removido) página duplicada
            - `5 - Extração de Dados.py` - Extração e processamento (~600 linhas)
            - `6 - Documentacao.py` - Documentação (~3.900 linhas)
            
            **📊 Total:** ~33.000+ linhas de código
            """)
        
        with col3:
            st.markdown(f"""
            ### 🔧 Tecnologias
            
            **Stack Principal:**
            - Streamlit {st.__version__}
            - Pandas {pd.__version__}
            - NumPy {np.__version__}
            - Altair (versão instalada)
            - Plotly (versão instalada)
            - OpenPyXL (versão instalada)
            """)

# ==========================================
# TC VEÍCULOS: ESPECIFICAÇÃO TÉCNICA
# ==========================================
elif indice_selecionado == "🧾 Especificação Técnica" and modulo_doc == "🚗 TC Veículos":
    st.header("🧾 Especificação Técnica — TC Veículos")

    st.markdown(
        """
        Especificação técnica completa do módulo **TC Veículos** em formato Markdown.
        Arquivo fonte: `DOCUMENTACAO_TC_PRINCIPAL.md`
        """
    )

    _caminho_doc_tc = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")

    if not os.path.exists(_caminho_doc_tc):
        st.error(f"Arquivo não encontrado: {_caminho_doc_tc}")
    else:
        try:
            with open(_caminho_doc_tc, "r", encoding="utf-8") as _f:
                _conteudo_tc = _f.read()

            st.download_button(
                label="📥 Baixar especificação TC Veículos (Markdown)",
                data=_conteudo_tc.encode("utf-8"),
                file_name="DOCUMENTACAO_TC_PRINCIPAL.md",
                mime="text/markdown",
                use_container_width=True,
            )

            st.markdown("---")
            st.markdown(_conteudo_tc)
        except Exception as e:
            st.error(f"Erro ao carregar especificação TC Veículos: {e}")

# ==========================================
# SEÇÃO 3: ESPECIFICAÇÃO TÉCNICA (REESCRITA)
# ==========================================
elif indice_selecionado == "🧾 Especificação Técnica":
    st.header("🧾 Especificação Técnica — TC Extendido")

    st.markdown(
        """
        Esta seção consolida uma **especificação técnica completa** em formato Markdown.
        O objetivo é permitir que você reescreva o projeto com IA preservando:
        - funcionalidades
        - regras de cálculo (CPU/Flex Bud)
        - fontes de dados e contratos (schemas)
        - comportamento de filtros e gráficos
        """
    )

    caminho_doc = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")

    if not os.path.exists(caminho_doc):
        st.error(f"Arquivo não encontrado: {caminho_doc}")
    else:
        try:
            with open(caminho_doc, "r", encoding="utf-8") as f:
                conteudo = f.read()

            st.download_button(
                label="📥 Baixar especificação (Markdown)",
                data=conteudo.encode("utf-8"),
                file_name="DOCUMENTACAO_SISTEMA_TC.md",
                mime="text/markdown",
                use_container_width=True,
            )

            st.markdown("---")
            st.markdown(conteudo)
        except Exception as e:
            st.error(f"Erro ao carregar especificação: {e}")

# ==========================================
# TC VEÍCULOS: GUIA DE EXTRAÇÃO DE DADOS
# ==========================================
elif indice_selecionado == "📥 Guia de Extração de Dados" and modulo_doc == "🚗 TC Veículos":
    st.header("📥 Guia de Extração de Dados — TC Veículos")

    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
    <h2 style="color: white; margin: 0;">📥 Extração de Dados — TC Veículos</h2>
    <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Pipeline de processamento de dados do módulo TC Veículos
    </p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("📋 **Visão Geral do Pipeline**", expanded=True):
        st.markdown("""
        ### 🔄 Fluxo de Processamento

        ```
        Arquivos Excel (Entrada)
            │
            ├──> processamento_dados_BUD.py
            │       ├──> Lê dados brutos do Budget
            │       ├──> Normaliza colunas e períodos
            │       ├──> Calcula composição de custos
            │       ├──> Grava df_principal_BUD.parquet
            │       └──> Chama processamento_dados_veiculos_BUD.py
            │               ├──> Rateio por veículo (tempo de produção)
            │               ├──> Cálculo de CPU por veículo
            │               ├──> Grava df_veiculos_custo_fp_BUD.parquet
            │               └──> Grava df_veiculos_cpu_BUD.parquet
            │
            └──> processamento_dados.py
                    ├──> Lê dados reais do Sapiens
                    ├──> Normaliza e processa
                    ├──> Grava df_principal.parquet
                    └──> Grava df_veiculos_custo_fp.parquet
        ```
        """)

    with st.expander("📂 **Arquivos de Processamento**", expanded=False):
        st.markdown("""
        ### 📋 Scripts e Funções

        | Arquivo | Função |
        |---------|--------|
        | `processamento_dados_BUD.py` | Processa dados Budget (principal + veículos) |
        | `processamento_dados_veiculos_BUD.py` | Rateio por veículo + CPU |
        | `processamento_dados.py` | Processa dados Real (Sapiens) |

        ### 📁 Pastas de Entrada
        - `dados/TC_Principal/{ano}/` — Arquivos Excel de entrada

        ### 📁 Pastas de Saída (Parquets processados)
        - `dados/TC_Principal/{ano}/BUD/` — Budget processado
        - `dados/TC_Principal/{ano}/` — Real processado
        """)

    with st.expander("🔧 **Detalhes do Processamento BUD**", expanded=False):
        st.markdown("""
        ### processamento_dados_BUD.py

        **Etapas:**
        1. **Leitura**: Excel com dados de Budget do TC Veículos
        2. **Normalização**: Padronização de colunas (Oficina, Veículo, Período, etc.)
        3. **Composição de Custos**:
           - Despesa Primária (soma dos lançamentos)
           - Custo FA = Despesa Primária × Rateio FA
           - Custo FP = Despesa Primária + Custo FA
           - D&A Dedicado (identificado por Account)
           - FP sem Dedicada = Custo FP − D&A Dedicado
        4. **Gravação**: `df_principal_BUD.parquet`
        5. **Chamada**: `processamento_dados_veiculos_BUD.py`

        ### processamento_dados_veiculos_BUD.py

        **Etapas:**
        1. **Leitura** do tempo de produção por veículo e oficina
        2. **Cálculo do percentual** de rateio por veículo
        3. **Rateio**: FP sem Dedicada × Percentual = Custo Rateado
        4. **Custo FP Veículo** = Custo Rateado + D&A Dedicado
        5. **CPU** = Custo FP Veículo / Volume
        6. **Gravação**: `df_veiculos_custo_fp_BUD.parquet`, `df_veiculos_cpu_BUD.parquet`
        """)

    with st.expander("📊 **Dados de Volume**", expanded=False):
        st.markdown("""
        ### Volumes por Veículo

        | Arquivo | Descrição |
        |---------|-----------|
        | `df_vol_veiculos_BUD.parquet` | Volume Budget por veículo |
        | `df_vol_veiculos_actual.parquet` | Volume Real por veículo |

        **Colunas:** `Oficina`, `Veículo`, `Período`, `Volume`

        Os volumes são usados para:
        - Cálculo de CPU
        - Cálculo de Flex Budget (proporção Real/BUD)
        - Gráficos comparativos (BUD vs Real)
        """)

# ==========================================
# SEÇÃO 4: GUIA DE EXTRAÇÃO DE DADOS
# ==========================================
elif indice_selecionado == "📥 Guia de Extração de Dados":
    st.header("📥 Guia de Extração de Dados — TC Extendido")
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
    <h2 style="color: white; margin: 0;">📚 Documentação Completa para IA</h2>
    <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Todos os Relacionamentos, Processos e Estruturas de Dados
    </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Índice interno
    st.markdown("## 📋 Índice do Guia")
    st.markdown("""
    ### 📖 Capítulo 1: Estrutura e Processamento dos Notebooks
    1. [Visão Geral](#visao-geral)
    2. [Notebook tc_ext/notebooks/dados.ipynb - Dados REAIS](#dados-reais)
    3. [Notebook tc_ext/notebooks/dados_BUD.ipynb - Dados BUDGET](#dados-budget)
    4. [Estrutura de Arquivos de Entrada](#estrutura-entrada)
    5. [Relacionamentos e Merges](#relacionamentos)
    6. [Colunas e Estrutura Final](#colunas-finais)
    7. [Consolidação do Histórico](#consolidacao)
    8. [Arquivos de Saída](#arquivos-saida)
    9. [Fluxo Completo](#fluxo-completo)
    10. [Tratamento de Erros](#tratamento-erros)
    11. [Checklist para Manutenção](#checklist)
    
    ### 🔄 Capítulo 2: Funcionamento da Atualização e Extração
    1. [Visão Geral do Processo de Atualização](#visao-atualizacao)
    2. [Ordem Cronológica dos Eventos](#ordem-cronologica)
    3. [Sistema de Busca de Arquivos](#busca-arquivos)
    4. [Criação de Pastas e Estrutura](#criacao-pastas)
    5. [Sistema de Upload de Arquivos](#sistema-upload)
    6. [Processamento e Execução](#processamento-execucao)
    7. [Cenários de Uso](#cenarios-uso)
    """)
    
    st.markdown("---")
    
    # ==========================================
    # CAPÍTULO 1: ESTRUTURA E PROCESSAMENTO DOS NOTEBOOKS
    # ==========================================
    
    with st.expander("📖 **Capítulo 1: Estrutura e Processamento dos Notebooks**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">📖 Capítulo 1: Estrutura e Processamento dos Notebooks</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Documentação Completa dos Notebooks de Extração - Estrutura, Processamento e Relacionamentos
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Seção 1: Visão Geral
        st.markdown("## 🎯 VISÃO GERAL {#visao-geral}")
        
        st.markdown("### Objetivo dos Notebooks")
        st.markdown("""
        Os notebooks `tc_ext/notebooks/dados.ipynb` e `tc_ext/notebooks/dados_BUD.ipynb` são responsáveis por:
        - **Carregar** dados de múltiplas fontes (Excel: SAPIENS, Reporting fluxo anexo)
        - **Processar** e **normalizar** dados de diferentes formatos e guias
        - **Unificar** informações através de merges por chaves comuns
        - **Calcular** rateios por veículo (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)
        - **Gerar** arquivos Parquet e Excel otimizados para uso no dashboard
        - **Consolidar** dados históricos para análises multi-anos
        """)
        
        st.markdown("### Diferença entre tc_ext/notebooks/dados.ipynb e tc_ext/notebooks/dados_BUD.ipynb")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **📊 tc_ext/notebooks/dados.ipynb - Dados REAIS**
        - Processa dados de custos **reais** (executados)
        - Lê guia **"Sapiens"** do Reporting fluxo anexo.xlsx
        - Lê guia **"Rateio"** para rateio por veículo
        - Lê guia **"Volume"** para volumes
        - Salva em: `dados/{ANO}/`
        - Histórico: `dados/historico_consolidado/`
        """)
        
        with col2:
            st.markdown("""
            **📈 tc_ext/notebooks/dados_BUD.ipynb - Dados BUDGET**
        - Processa dados de **orçamento/planejamento** (Budget)
        - Lê guia **"Voz de custo BDG"** do Reporting fluxo anexo.xlsx
        - Lê guia **"Rateio BDG"** para rateio por veículo
        - Lê guia **"Volume BDG"** para volumes
        - Salva em: `dados/{ANO}/BUD/`
        - Histórico: `dados/historico_consolidado/BUD/`
        """)
        
        st.markdown("### Fluxo Principal")
        st.code("""
        Arquivos Excel (Entrada)
            │
            ├──> tc_ext/notebooks/dados.ipynb (REAL)
            │       ├──> Processamento
            │       ├──> Merges (Account, Nº conta, Centro cst, Oficina+Período)
            │       ├──> Cálculo Rateio por Veículo
            │       ├──> Merge com Volume
            │       └──> Salvar Parquet + Consolidar Histórico
            │
            └──> tc_ext/notebooks/dados_BUD.ipynb (BUDGET)
                    ├──> Processamento (mesma lógica)
                    ├──> Merges (mesmas chaves)
                    ├──> Cálculo Rateio por Veículo
                    ├──> Merge com Volume
                    └──> Salvar Parquet (BUD) + Consolidar Histórico (BUD)
        """, language="text")
        
        st.markdown("---")
        
        # Seção 2: tc_ext/notebooks/dados.ipynb - Dados REAIS
        st.markdown("## 📊 NOTEBOOK tc_ext/notebooks/dados.ipynb - DADOS REAIS {#dados-reais}")
        
        st.markdown("### Estrutura do Processamento")
        
        with st.expander("🔧 **Célula 0: Configuração Inicial**", expanded=False):
            st.markdown("""
            **Objetivo**: Configurar ano, pastas e caminhos
            
            **Processo**:
            1. Solicita ano para processar (padrão: ano atual)
            2. Cria estrutura de pastas:
               - `dados/{ANO_ATUAL}/` - Dados do ano específico
               - `dados/historico_consolidado/` - Histórico consolidado
            3. Verifica arquivos de entrada:
               - `Dados SAPIENS.xlsx`
               - `Reporting fluxo anexo.xlsx`
            4. Define caminhos de entrada e saída
            
            **Variáveis Criadas**:
            - `ANO_ATUAL`: Ano selecionado para processamento
            - `PASTA_ANO`: `dados/{ANO_ATUAL}/`
            - `PASTA_HISTORICO`: `dados/historico_consolidado/`
            - `CAMINHO_SAPIENS`: Caminho para Dados SAPIENS.xlsx
            - `CAMINHO_RATEIO`: Caminho para Reporting fluxo anexo.xlsx
            - `CAMINHO_DF_FINAL`: `dados/{ANO}/df_final.parquet`
            - `CAMINHO_DF_VOL`: `dados/{ANO}/df_vol.parquet`
            - `CAMINHO_DF_KE5Z_GROUP`: `dados/{ANO}/df_ke5z_group.parquet`
            """)
        
        with st.expander("📥 **Célula 1: Leitura dos Dados SAPIENS (KE5Z)**", expanded=False):
            st.markdown("""
            **Arquivo**: `Reporting fluxo anexo.xlsx`
            **Guia**: `"Sapiens"`
            **Cabeçalho**: Linha 1 (`header=1`)
            **Colunas**: A até T (20 colunas, `usecols=range(20)`)
            
            **Colunas Lidas**:
            - `Mes`, `Período`, `Nºconta`, `Centrocst`, `Nºdoc.ref.`, `Dt.lçto.`
            - `Valor`, `QTD`, `Type 05`, `Type 06`, `Account` (Type 07)
            - `USI`, `Oficina`, `Doc.compra`, `Texto breve`
            - `Fornecedor`, `Material`, `Usuário`, `Fornec.`, `Tipo`
            
            **DataFrame Criado**: `df_KE5Z`
            
            **Validação**: Soma da coluna `Valor` para verificar leitura
            """)
        
        with st.expander("🔗 **Célula 2: Merge com Base Conso (Custo)**", expanded=False):
            st.markdown("""
            **Arquivo**: `Dados SAPIENS.xlsx`
            **Guia**: `"Base conso"`
            
            **Processo**:
            1. Lê guia "Base conso"
            2. Renomeia `Type 04` → `Custo` (se existir)
            3. Mantém apenas colunas: `Custo`, `Type 07`
            4. Renomeia `Type 07` → `Account`
            5. Faz merge com `df_KE5Z` usando `Account` como chave
            
            **Chave de Merge**: `Account` (Type 07)
            **Tipo**: `left` (mantém todos os registros de KE5Z)
            
            **Resultado**: Adiciona coluna `Custo` ao `df_KE5Z`
            - Valores possíveis: `"Variável"` ou `"Fixo"`
            """)
        
        with st.expander("📊 **Célula 3: Processamento de Rateio**", expanded=False):
            st.markdown("""
            **Arquivo**: `Reporting fluxo anexo.xlsx`
            **Guia**: `"Rateio"`
            
            **Processo**:
            1. Lê guia sem header (`header=None`)
            2. Remove primeira linha (linha de referência)
            3. Usa segunda linha como cabeçalho (meses)
            4. Remove linha usada como cabeçalho
            5. Identifica colunas de meses (janeiro a dezembro)
            6. Usa `melt()` para transformar colunas de meses em linhas
            7. Cria colunas: `Período` (mês) e `Rateio` (valor)
            8. Normaliza `Período` para capitalizado (Janeiro, Fevereiro, etc.)
            9. Filtra: Remove `Oficina == 'Veículos'` e linhas com `Oficina` NaN
            
            **Colunas de Identificação (id_vars)**:
            - `Oficina`, `Veículo` (e outras colunas não-mês)
            
            **Colunas Transformadas (value_vars)**:
            - Meses: Janeiro, Fevereiro, Março, ..., Dezembro
            
            **DataFrame Criado**: `df` (com colunas: `Oficina`, `Veículo`, `Período`, `Rateio`)
            """)
        
        with st.expander("🔄 **Célula 4: Merge KE5Z ↔ Rateio e Cálculo por Veículo**", expanded=False):
            st.markdown("""
            **Chave de Merge**: `['Oficina', 'Período']` (COMPOSTA)
            **Tipo**: `left` (mantém todos os registros de KE5Z)
            
            **Processo**:
            1. Merge `df_KE5Z` com `df` (rateio) usando `['Oficina', 'Período']`
            2. Pivot: Transforma `Veículo` em colunas de `Rateio`
               - Index: `['Oficina', 'Período']`
               - Columns: `Veículo` (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)
               - Values: `Rateio` (percentuais)
               - Aggfunc: `mean` (média para agregar duplicatas)
            3. Renomeia colunas de veículos: adiciona `%` (ex: `CC21%`, `CC22%`)
            4. Merge reverso: `df_KE5Z` com `df_pivot` usando `['Oficina', 'Período']`
            5. Calcula colunas de valores por veículo:
               - `CC21 = CC21% * Valor`
               - `CC22 = CC22% * Valor`
               - `CC24 = CC24% * Valor`
               - `CC24 5L = CC24 5L% * Valor`
               - `CC24 7L = CC24 7L% * Valor`
               - `J516 = J516% * Valor`
            6. Calcula `Soma_Percentuais = CC21% + CC22% + ... + J516%`
            7. Remove colunas de percentual (`CC21%`, `CC22%`, etc.)
            
            **Resultado**: `df_final` com colunas de valores por veículo calculadas
            """)
        
        with st.expander("📈 **Célula 5: Processamento de Volume**", expanded=False):
            st.markdown("""
            **Arquivo**: `Reporting fluxo anexo.xlsx`
            **Guia**: `"Volume"`
            **Cabeçalho**: Linha 51 (`header=50`, 0-indexed)
            
            **Processo**:
            1. Lê guia "Volume" com cabeçalho na linha 51
            2. Identifica colunas de meses (janeiro a dezembro)
            3. Usa `melt()` para transformar colunas de meses em linhas
            4. Cria colunas: `Período` (mês) e `Volume` (valor)
            5. Normaliza `Período` para capitalizado
            6. Converte `Volume` para numérico
            7. Remove linhas onde `Oficina` ou `Período` são NaN
            8. Preenche NaN em `Volume` com 0
            9. Remove duplicatas
            
            **Colunas Finais**: `Oficina`, `Veículo`, `Período`, `Volume`
            
            **DataFrame Criado**: `df_vol`
            """)
        
        with st.expander("🔗 **Célula 6: Merge df_final ↔ df_vol (Volume)**", expanded=False):
            st.markdown("""
            **Chave de Merge**: `['Oficina', 'Período', 'Veículo']` (COMPOSTA)
            **Tipo**: `left` (mantém todos os registros de df_final)
            
            **Processo**:
            1. Verifica se colunas de chave existem em ambos DataFrames
            2. Faz merge adicionando coluna `Volume` ao `df_final`
            3. Preenche NaN em `Volume` com 0 (se não houver match)
            
            **Resultado**: `df_final` com coluna `Volume` adicionada
            """)
        
        with st.expander("💾 **Célula 7: Salvamento e Consolidação**", expanded=False):
            st.markdown("""
            **Arquivos Salvos (Pasta do Ano)**:
            1. `df_final.parquet` - Dados completos com rateio por veículo e volume
            2. `df_vol.parquet` - Dados de volume
            3. `df_ke5z_group.parquet` - Dados agrupados (se aplicável)
            
            **Consolidação do Histórico**:
            1. Carrega histórico existente (se existir):
               - `dados/historico_consolidado/df_final_historico.parquet`
               - `dados/historico_consolidado/df_vol_historico.parquet`
            2. Adiciona coluna `Ano` aos dados do ano atual
            3. Concatena dados do ano atual com histórico existente
            4. Remove duplicatas (se houver)
            5. Salva histórico atualizado:
               - `dados/historico_consolidado/df_final_historico.parquet`
               - `dados/historico_consolidado/df_vol_historico.parquet`
            
            **IMPORTANTE**: O histórico é sempre **concatenado**, nunca substituído
            """)
        
        st.markdown("---")
        
        # Seção 3: tc_ext/notebooks/dados_BUD.ipynb - Dados BUDGET
        st.markdown("## 📈 NOTEBOOK tc_ext/notebooks/dados_BUD.ipynb - DADOS BUDGET {#dados-budget}")
        
        st.markdown("### Diferenças Principais em Relação a tc_ext/notebooks/dados.ipynb")
        
        diferencas_bud = {
            "Aspecto": [
                "Guia de Dados Principais",
                "Guia de Rateio",
                "Guia de Volume",
                "Pasta de Saída",
                "Sufixo dos Arquivos",
                "Pasta de Histórico"
            ],
            "tc_ext/notebooks/dados.ipynb (REAL)": [
                '"Sapiens"',
                '"Rateio"',
                '"Volume"',
                "dados/{ANO}/",
                "Sem sufixo",
                "dados/historico_consolidado/"
            ],
            "tc_ext/notebooks/dados_BUD.ipynb (BUDGET)": [
                '"Voz de custo BDG"',
                '"Rateio BDG"',
                '"Volume BDG"',
                "dados/{ANO}/BUD/",
                "_BUD (ex: df_final_BUD.parquet)",
                "dados/historico_consolidado/BUD/"
            ]
        }
        
        st.dataframe(pd.DataFrame(diferencas_bud), use_container_width=True, hide_index=True)
        
        st.markdown("### Processo Idêntico")
        st.info("""
        **IMPORTANTE**: O processo de processamento, merges, cálculos e consolidação
        é **IDÊNTICO** ao `tc_ext/notebooks/dados.ipynb`. A única diferença são as guias lidas e os
        caminhos de saída. Todas as transformações, relacionamentos e cálculos seguem
        a mesma lógica.
        """)
        
        st.markdown("---")
        
        # Seção 4: Estrutura de Arquivos de Entrada
        st.markdown("## 📁 ESTRUTURA DE ARQUIVOS DE ENTRADA {#estrutura-entrada}")
        
        st.markdown("### Arquivos Necessários")
        
        with st.expander("📊 **Reporting fluxo anexo.xlsx**", expanded=False):
            st.markdown("""
            **Localização**: `dados/{ANO}/Reporting fluxo anexo.xlsx` ou raiz do projeto
            
            **Guias Utilizadas (tc_ext/notebooks/dados.ipynb - REAL)**:
            1. **"Sapiens"** (Célula 1)
               - Cabeçalho: Linha 1
               - Colunas: A até T (20 colunas)
               - Dados: Custos reais executados
            
            2. **"Rateio"** (Célula 3)
               - Cabeçalho: Segunda linha (após linha de referência)
               - Colunas de meses: Janeiro a Dezembro
               - Dados: Percentuais de rateio por Oficina, Veículo e Período
            
            3. **"Volume"** (Célula 5)
               - Cabeçalho: Linha 51
               - Colunas de meses: Janeiro a Dezembro
               - Dados: Volumes por Oficina, Veículo e Período
            
            **Guias Utilizadas (tc_ext/notebooks/dados_BUD.ipynb - BUDGET)**:
            1. **"Voz de custo BDG"** (equivalente a "Sapiens")
            2. **"Rateio BDG"** (equivalente a "Rateio")
            3. **"Volume BDG"** (equivalente a "Volume")
            """)
        
        with st.expander("📋 **Dados SAPIENS.xlsx**", expanded=False):
            st.markdown("""
            **Localização**: `dados/{ANO}/Dados SAPIENS.xlsx` ou raiz do projeto
            
            **Guias Utilizadas**:
            1. **"Base conso"**
               - Colunas: `Type 04` (renomeado para `Custo`), `Type 07` (renomeado para `Account`)
               - Propósito: Mapear Account para tipo de custo (Variável/Fixo)
               - Chave de merge: `Account` (Type 07)
            
            **Observação**: Este arquivo é usado tanto em `tc_ext/notebooks/dados.ipynb` quanto em `tc_ext/notebooks/dados_BUD.ipynb`
            """)
        
        st.markdown("---")
        
        # Seção 5: Relacionamentos e Merges
        st.markdown("## 🔗 RELACIONAMENTOS E MERGES {#relacionamentos}")
        
        st.markdown("### Resumo de Todos os Merges")
        
        resumo_merges = {
            "Merge": [
                "KE5Z ↔ Base Conso",
                "KE5Z ↔ Rateio",
                "KE5Z ↔ Volume",
                "Histórico ↔ Ano Atual"
            ],
            "Chave KE5Z": [
                "Account (Type 07)",
                "['Oficina', 'Período']",
                "['Oficina', 'Período', 'Veículo']",
                "N/A (concatenação)"
            ],
            "Chave Externa": [
                "Account (Type 07)",
                "['Oficina', 'Período']",
                "['Oficina', 'Período', 'Veículo']",
                "N/A (concatenação)"
            ],
            "Tipo": [
                "left",
                "left",
                "left",
                "concat"
            ],
            "Resultado": [
                "Coluna Custo (Variável/Fixo)",
                "Colunas de rateio por veículo (CC21%, CC22%, etc.)",
                "Coluna Volume",
                "Histórico consolidado com todos os anos"
            ]
        }
        
        st.dataframe(pd.DataFrame(resumo_merges), use_container_width=True, hide_index=True)
        
        st.markdown("### Detalhamento dos Merges")
        
        with st.expander("1. Merge KE5Z ↔ Base Conso (Custo)", expanded=False):
            st.code("""
# Leitura
df_base_conso = pd.read_excel('Dados SAPIENS.xlsx', sheet_name='Base conso')
df_base_conso = df_base_conso.rename(columns={'Type 04': 'Custo', 'Type 07': 'Account'})
df_base_conso = df_base_conso[['Custo', 'Account']]

# Merge
df_KE5Z = pd.merge(df_KE5Z, df_base_conso, on='Account', how='left')
            """, language="python")
            
            st.markdown("""
            **Resultado**: Adiciona coluna `Custo` ao `df_KE5Z`
            - Valores: `"Variável"` ou `"Fixo"`
            - Usado para cálculos de Flex Bud e análises de custos fixos vs variáveis
            """)
        
        with st.expander("2. Merge KE5Z ↔ Rateio (Percentuais por Veículo)", expanded=False):
            st.code("""
# Processamento do Rateio
df_rateio = pd.read_excel('Reporting fluxo anexo.xlsx', sheet_name='Rateio', header=None)
# ... processamento com melt() ...
df_pivot = df_rateio.pivot_table(
        index=['Oficina', 'Período'],
        columns='Veículo',
        values='Rateio',
        aggfunc='mean'
).reset_index()

# Renomear colunas de veículos para adicionar %
veiculos_cols = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
rename_dict = {col: f"{col}%" for col in veiculos_cols}
df_pivot = df_pivot.rename(columns=rename_dict)

# Merge
df_final = pd.merge(df_KE5Z, df_pivot, on=['Oficina', 'Período'], how='left')
            """, language="python")
            
            st.markdown("""
            **Resultado**: Adiciona colunas de percentuais por veículo
            - `CC21%`, `CC22%`, `CC24%`, `CC24 5L%`, `CC24 7L%`, `J516%`
            - Valores: Percentuais (0.0 a 1.0 ou 0% a 100%)
            - Usado para calcular valores por veículo: `CC21 = CC21% * Valor`
            """)
        
        with st.expander("3. Merge df_final ↔ Volume", expanded=False):
            st.code("""
# Processamento do Volume
df_vol = pd.read_excel('Reporting fluxo anexo.xlsx', sheet_name='Volume', header=50)
# ... processamento com melt() ...
# Colunas finais: Oficina, Veículo, Período, Volume

# Merge
df_final = pd.merge(df_final, df_vol, on=['Oficina', 'Período', 'Veículo'], how='left')
df_final['Volume'] = df_final['Volume'].fillna(0)
            """, language="python")
            
            st.markdown("""
            **Resultado**: Adiciona coluna `Volume` ao `df_final`
            - Valores: Volumes numéricos por veículo
            - Usado para cálculos de CPU (Custo por Unidade)
            """)
        
        st.markdown("---")
        
        # Seção 6: Colunas e Estrutura Final
        st.markdown("## 📊 COLUNAS E ESTRUTURA FINAL {#colunas-finais}")
        
        st.markdown("### Colunas do DataFrame Final (df_final.parquet)")
        
        colunas_finais = {
            "Coluna": [
                "Mes", "Período", "Ano",
                "Nºconta", "Centrocst", "Nºdoc.ref.", "Dt.lçto.",
                "Valor", "QTD", "Volume",
                "Type 05", "Type 06", "Account", "Custo",
                "USI", "Oficina",
                "Doc.compra", "Texto breve",
                "Fornecedor", "Material", "Usuário", "Fornec.", "Tipo",
                "CC21", "CC22", "CC24", "CC24 5L", "CC24 7L", "J516",
                "Soma_Percentuais"
            ],
            "Tipo": [
                "float64", "object", "int64",
                "object", "object", "float64", "object",
                "float64", "float64", "float64",
                "object", "object", "object", "object",
                "object", "object",
                "object", "object",
                "object", "object", "object", "object", "object",
                "float64", "float64", "float64", "float64", "float64", "float64",
                "float64"
            ],
            "Origem": [
                "Sapiens", "Sapiens", "Adicionado na consolidação",
                "Sapiens", "Sapiens", "Sapiens", "Sapiens",
                "Sapiens", "Sapiens", "Volume (merge)",
                "Sapiens", "Sapiens", "Sapiens", "Base conso (merge)",
                "Sapiens", "Sapiens",
                "Sapiens", "Sapiens",
                "Sapiens", "Sapiens", "Sapiens", "Sapiens", "Sapiens",
                "Calculado (CC21% * Valor)", "Calculado", "Calculado", "Calculado", "Calculado", "Calculado",
                "Calculado (soma dos %)"
            ],
            "Descrição": [
                "Mês numérico (1-12)", "Mês por extenso (Janeiro, etc.)", "Ano do registro",
                "Código da conta contábil", "Centro de custo", "Número documento referência", "Data de lançamento",
                "Valor monetário do custo", "Quantidade", "Volume do veículo",
                "Classificação Type 05", "Classificação Type 06", "Account (Type 07)", "Tipo de custo (Variável/Fixo)",
                "Unidade de negócio", "Nome da oficina",
                "Documento de compra", "Descrição breve do material",
                "Nome do fornecedor", "Código do material", "Usuário", "Código fornecedor", "Tipo de lançamento",
                "Valor rateado para CC21", "Valor rateado para CC22", "Valor rateado para CC24", "Valor rateado para CC24 5L", "Valor rateado para CC24 7L", "Valor rateado para J516",
                "Soma de todos os percentuais (validação)"
            ]
        }
        
        st.dataframe(pd.DataFrame(colunas_finais), use_container_width=True, hide_index=True)
        
        st.markdown("### Colunas do DataFrame de Volume (df_vol.parquet)")
        
        colunas_volume = {
            "Coluna": ["Oficina", "Veículo", "Período", "Volume"],
            "Tipo": ["object", "object", "object", "float64"],
            "Descrição": [
                "Nome da oficina",
                "Código do veículo (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)",
                "Mês por extenso (Janeiro, Fevereiro, etc.)",
                "Volume numérico do veículo no período"
            ]
        }
        
        st.dataframe(pd.DataFrame(colunas_volume), use_container_width=True, hide_index=True)
        
        st.markdown("### Relacionamento entre Colunas")
        
        st.markdown("""
        **Chaves Primárias para Merges**:
        - `Account` (Type 07) → Merge com Base Conso
        - `['Oficina', 'Período']` → Merge com Rateio
        - `['Oficina', 'Período', 'Veículo']` → Merge com Volume
        
        **Colunas Calculadas**:
        - `CC21 = CC21% * Valor` (e similares para outros veículos)
        - `Soma_Percentuais = CC21% + CC22% + CC24% + CC24 5L% + CC24 7L% + J516%`
        - `CPU = Valor / Volume` (calculado no app.py, não no notebook)
        
        **Normalizações Críticas**:
        - `Período`: Sempre capitalizado (Janeiro, Fevereiro, etc.)
        - `Account`: Mantido como string/object
        - `Volume`: Sempre numérico (float64), NaN preenchido com 0
        """)
        
        st.markdown("---")
        
        # Seção 7: Consolidação do Histórico
        st.markdown("## 📚 CONSOLIDAÇÃO DO HISTÓRICO {#consolidacao}")
        
        st.markdown("### Processo de Consolidação")
        
        st.markdown("""
        **Objetivo**: Manter um histórico completo de todos os anos processados
        
        **Processo**:
        1. **Verificar histórico existente**:
           - Tenta carregar `dados/historico_consolidado/df_final_historico.parquet`
           - Se não existir, cria DataFrame vazio
        
        2. **Adicionar coluna Ano**:
           - Adiciona `Ano = ANO_ATUAL` aos dados do ano atual
           - Garante que cada registro tenha identificação do ano
        
        3. **Concatenação**:
           - Concatena dados do ano atual com histórico existente
           - Usa `pd.concat([df_historico, df_ano_atual], ignore_index=True)`
        
        4. **Validação**:
           - Verifica se `Volume` é sempre numérico
           - Garante tipos de dados consistentes
        
        5. **Salvamento**:
           - Salva histórico atualizado
           - Mantém histórico sempre completo
        
        **IMPORTANTE**: 
        - O histórico é **sempre concatenado**, nunca substituído
        - Permite análises multi-anos no dashboard
        - O sistema prioriza o histórico consolidado para carregar dados
        """)
        
        st.markdown("### Estrutura do Histórico")
        
        st.code("""
        dados/historico_consolidado/
        ├── df_final_historico.parquet      # Todos os anos de custos (REAL)
        ├── df_vol_historico.parquet        # Todos os anos de volumes
        ├── df_ke5z_historico.parquet       # Dados KE5Z agrupados
        └── BUD/
            ├── df_final_historico_BUD.parquet  # Todos os anos de custos (BUDGET)
            ├── df_vol_historico_BUD.parquet    # Todos os anos de volumes (BUDGET)
            └── df_ke5z_historico_BUD.parquet   # Dados KE5Z agrupados (BUDGET)
        """, language="text")
        
        st.markdown("---")
        
        # Seção 8: Arquivos de Saída
        st.markdown("## 💾 ARQUIVOS DE SAÍDA {#arquivos-saida}")
        
        st.markdown("### Arquivos Gerados por tc_ext/notebooks/dados.ipynb (REAL)")
        
        arquivos_saida_real = {
            "Arquivo": [
                "df_final.parquet",
                "df_vol.parquet",
                "df_ke5z_group.parquet",
                "df_final_historico.parquet",
                "df_vol_historico.parquet",
                "df_ke5z_historico.parquet"
            ],
            "Localização": [
                "dados/{ANO}/",
                "dados/{ANO}/",
                "dados/{ANO}/",
                "dados/historico_consolidado/",
                "dados/historico_consolidado/",
                "dados/historico_consolidado/"
            ],
            "Conteúdo": [
                "Dados completos com rateio por veículo e volume",
                "Dados de volume por Oficina, Veículo e Período",
                "Dados agrupados KE5Z",
                "Histórico consolidado de todos os anos (REAL)",
                "Histórico consolidado de volumes",
                "Histórico consolidado KE5Z"
            ],
            "Uso": [
                "Dashboard principal (app.py)",
                "Cálculos de CPU e análises de volume",
                "Análises específicas",
                "Análises multi-anos",
                "Análises multi-anos de volume",
                "Análises históricas KE5Z"
            ]
        }
        
        st.dataframe(pd.DataFrame(arquivos_saida_real), use_container_width=True, hide_index=True)
        
        st.markdown("### Arquivos Gerados por tc_ext/notebooks/dados_BUD.ipynb (BUDGET)")
        
        arquivos_saida_bud = {
            "Arquivo": [
                "df_final_BUD.parquet",
                "df_vol_BUD.parquet",
                "df_ke5z_group_BUD.parquet",
                "df_final_historico_BUD.parquet",
                "df_vol_historico_BUD.parquet",
                "df_ke5z_historico_BUD.parquet"
            ],
            "Localização": [
                "dados/{ANO}/BUD/",
                "dados/{ANO}/BUD/",
                "dados/{ANO}/BUD/",
                "dados/historico_consolidado/BUD/",
                "dados/historico_consolidado/BUD/",
                "dados/historico_consolidado/BUD/"
            ],
            "Conteúdo": [
                "Dados de Budget com rateio por veículo e volume",
                "Dados de volume de Budget",
                "Dados agrupados KE5Z (Budget)",
                "Histórico consolidado de todos os anos (BUDGET)",
                "Histórico consolidado de volumes (Budget)",
                "Histórico consolidado KE5Z (Budget)"
            ],
            "Uso": [
                "Comparações Real vs Budget",
                "Análises de volume Budget",
                "Análises específicas Budget",
                "Análises multi-anos Budget",
                "Análises multi-anos de volume Budget",
                "Análises históricas KE5Z Budget"
            ]
        }
        
        st.dataframe(pd.DataFrame(arquivos_saida_bud), use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Seção 9: Fluxo Completo
        st.markdown("## 🔄 FLUXO COMPLETO {#fluxo-completo}")
        
        st.markdown("### Diagrama de Fluxo - tc_ext/notebooks/dados.ipynb")
        
        st.code("""
        ┌─────────────────────────────────────┐
        │  Configuração (Célula 0)           │
        │  - Define ANO_ATUAL                 │
        │  - Cria pastas                      │
        │  - Verifica arquivos                │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Leitura SAPIENS (Célula 1)        │
        │  - Reporting fluxo anexo.xlsx       │
        │  - Guia "Sapiens"                   │
        │  - Cria df_KE5Z                     │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Merge Base Conso (Célula 2)       │
        │  - Dados SAPIENS.xlsx                │
        │  - Guia "Base conso"                 │
        │  - Adiciona coluna Custo             │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Processamento Rateio (Célula 3)   │
        │  - Reporting fluxo anexo.xlsx         │
        │  - Guia "Rateio"                     │
        │  - Transforma meses em linhas         │
        │  - Cria df (Oficina, Veículo,        │
        │    Período, Rateio)                  │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Merge + Cálculo Veículos (Célula 4)│
        │  - Merge KE5Z ↔ Rateio               │
        │  - Pivot: Veículo → Colunas          │
        │  - Calcula: CC21 = CC21% * Valor     │
        │  - Cria df_final                     │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Processamento Volume (Célula 5)   │
        │  - Reporting fluxo anexo.xlsx       │
        │  - Guia "Volume"                     │
        │  - Transforma meses em linhas         │
        │  - Cria df_vol                       │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Merge Volume (Célula 6)           │
        │  - Merge df_final ↔ df_vol           │
        │  - Adiciona coluna Volume             │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │  Salvamento + Consolidação (Célula 7)│
        │  - Salva df_final.parquet            │
        │  - Salva df_vol.parquet               │
        │  - Carrega histórico                  │
        │  - Concatena com ano atual            │
        │  - Salva histórico atualizado         │
        └─────────────────────────────────────┘
        """, language="text")
        
        st.markdown("### Sequência de Operações Detalhada")
        
        operacoes_detalhadas = [
            "**Célula 0**: Configuração - Define ano, cria pastas, verifica arquivos de entrada",
            "**Célula 1**: Leitura SAPIENS - Lê guia 'Sapiens' (20 colunas), cria df_KE5Z com dados de custos",
            "**Célula 2**: Merge Base Conso - Adiciona coluna 'Custo' (Variável/Fixo) usando Account como chave",
            "**Célula 3**: Processamento Rateio - Lê guia 'Rateio', transforma meses em linhas (melt), cria df com Oficina, Veículo, Período, Rateio",
            "**Célula 4**: Merge Rateio + Cálculo - Merge KE5Z ↔ Rateio, pivot de Veículo para colunas, calcula valores por veículo (CC21, CC22, etc.), cria df_final",
            "**Célula 5**: Processamento Volume - Lê guia 'Volume' (header=50), transforma meses em linhas, cria df_vol com Oficina, Veículo, Período, Volume",
            "**Célula 6**: Merge Volume - Merge df_final ↔ df_vol usando ['Oficina', 'Período', 'Veículo'], adiciona coluna Volume",
            "**Célula 7**: Salvamento - Salva df_final.parquet, df_vol.parquet na pasta do ano, carrega histórico, concatena, salva histórico consolidado"
        ]
        
        for op in operacoes_detalhadas:
            st.markdown(f"- {op}")
        
        st.markdown("---")
        
        # Seção 10: Tratamento de Erros
        st.markdown("## ⚠️ TRATAMENTO DE ERROS {#tratamento-erros}")
        
        st.markdown("### Erros Comuns e Soluções")
        
        with st.expander("1. Arquivo Não Encontrado", expanded=False):
            st.markdown("""
            **Sintoma**: `FileNotFoundError` ao tentar ler arquivo Excel
            
            **Soluções**:
            - Verificar se arquivo está em `dados/{ANO}/` ou na raiz do projeto
            - Verificar nomes exatos: `Dados SAPIENS.xlsx` e `Reporting fluxo anexo.xlsx`
            - O notebook tenta copiar da raiz para pasta do ano automaticamente
            """)
        
        with st.expander("2. Guia Não Encontrada", expanded=False):
            st.markdown("""
            **Sintoma**: `ValueError: Worksheet named 'X' not found`
            
            **Soluções**:
            - Verificar nomes exatos das guias (case-sensitive):
              - `tc_ext/notebooks/dados.ipynb`: "Sapiens", "Rateio", "Volume"
              - `tc_ext/notebooks/dados_BUD.ipynb`: "Voz de custo BDG", "Rateio BDG", "Volume BDG"
            - Verificar se guias existem no arquivo Excel
            """)
        
        with st.expander("3. Coluna Não Encontrada Após Merge", expanded=False):
            st.markdown("""
            **Sintoma**: `KeyError: 'Coluna X'` após merge
            
            **Soluções**:
            - Verificar se chaves de merge existem em ambos DataFrames
            - Verificar normalização de `Período` (deve estar capitalizado)
            - Verificar tipos de dados das chaves (devem ser compatíveis)
            - Verificar se merge foi feito com chaves corretas
            """)
        
        with st.expander("4. Volume NaN ou Zerado", expanded=False):
            st.markdown("""
            **Sintoma**: Coluna Volume com muitos NaN ou zeros
            
            **Soluções**:
            - Verificar se merge foi feito com chave composta correta: `['Oficina', 'Período', 'Veículo']`
            - Verificar se dados de volume existem para a combinação Oficina+Período+Veículo
            - Verificar normalização de `Período` (deve estar capitalizado em ambos DataFrames)
            - O notebook preenche NaN com 0 automaticamente
            """)
        
        with st.expander("5. Percentuais de Rateio Não Somam 100%", expanded=False):
            st.markdown("""
            **Sintoma**: `Soma_Percentuais` diferente de 1.0 (ou 100%)
            
            **Soluções**:
            - Verificar se todos os veículos estão incluídos no rateio
            - Verificar se há veículos não mapeados
            - Verificar se pivot foi feito corretamente (aggfunc='mean')
            - Validação: `Soma_Percentuais` deve estar próximo de 1.0
            """)
        
        with st.expander("6. Histórico Não Atualizado", expanded=False):
            st.markdown("""
            **Sintoma**: Histórico não inclui dados do ano atual após processamento
            
            **Soluções**:
            - Verificar se coluna `Ano` foi adicionada aos dados do ano atual
            - Verificar se concatenação foi executada corretamente
            - Verificar se arquivo de histórico foi salvo após concatenação
            - Verificar permissões de escrita na pasta `historico_consolidado/`
            """)
        
        st.markdown("### Validações Implementadas")
        
        st.markdown("""
        **Validações Automáticas**:
        1. **Validação de Arquivos**: Verifica existência antes de processar
        2. **Validação de Colunas**: Verifica se colunas essenciais existem antes de merge
        3. **Validação de Volume**: Garante que Volume seja sempre numérico
        4. **Validação de Período**: Normaliza para formato capitalizado
        5. **Validação de Histórico**: Verifica tipos de dados ao carregar histórico
        6. **Validação de Soma**: Calcula `Soma_Percentuais` para validar rateios
        """)
        
        st.markdown("---")
        
        # Seção 11: Checklist para Manutenção
        st.markdown("## ✅ CHECKLIST PARA MANUTENÇÃO {#checklist}")
        
        st.markdown("### Antes de Modificar os Notebooks")
        
        checklist_antes = [
            "Verificar se estrutura de pastas está correta",
            "Verificar se nomes de guias estão corretos",
            "Verificar se chaves de merge estão corretas",
            "Verificar se tipos de dados estão consistentes",
            "Verificar se normalização de Período está funcionando",
            "Verificar se cálculo de veículos está correto",
            "Verificar se consolidação de histórico está funcionando"
        ]
        
        for item in checklist_antes:
            st.markdown(f"- [ ] {item}")
        
        st.markdown("### Ao Modificar")
        
        checklist_modificar = [
            "Manter mesma estrutura de chaves de merge",
            "Manter normalização de Período (capitalizado)",
            "Manter tipos de dados consistentes (Volume sempre numérico)",
            "Manter lógica de cálculo de veículos (CC21 = CC21% * Valor)",
            "Manter processo de consolidação de histórico (concatenação, não substituição)",
            "Testar com dados de um ano antes de processar todos",
            "Validar que Volume não está sendo zerado incorretamente",
            "Validar que Soma_Percentuais está próximo de 1.0"
        ]
        
        for item in checklist_modificar:
            st.markdown(f"- [ ] {item}")
        
        st.markdown("### Após Modificar")
        
        checklist_depois = [
            "Verificar se arquivos Parquet foram gerados corretamente",
            "Verificar se histórico foi atualizado",
            "Verificar se Volume está presente e numérico",
            "Verificar se colunas de veículos foram calculadas",
            "Verificar se não há erros de tipo de dados",
            "Testar carregamento no app.py",
            "Validar que dados aparecem corretamente no dashboard"
        ]
        
        for item in checklist_depois:
            st.markdown(f"- [ ] {item}")
        
        st.markdown("### Regras Críticas que NUNCA Devem Ser Alteradas")
        
        st.warning("""
        **⚠️ ATENÇÃO**: As seguintes regras são CRÍTICAS e não devem ser alteradas sem
        análise profunda, pois podem quebrar todo o sistema:
        
        1. **Chaves de Merge**: `['Oficina', 'Período']` para Rateio e `['Oficina', 'Período', 'Veículo']` para Volume
        2. **Normalização de Período**: Sempre capitalizado (Janeiro, Fevereiro, etc.)
        3. **Cálculo de Veículos**: `CC21 = CC21% * Valor` (e similares)
        4. **Consolidação de Histórico**: Sempre concatenar, nunca substituir
        5. **Tipo de Volume**: Sempre numérico (float64), nunca object
        6. **Estrutura de Pastas**: `dados/{ANO}/` para ano específico, `dados/historico_consolidado/` para histórico
        7. **Sufixo BUD**: Arquivos de Budget sempre com sufixo `_BUD` e em pasta `BUD/`
        """)
        
        st.markdown("---")
        
        # Seção Final: Notas Importantes
        st.markdown("## 📝 NOTAS IMPORTANTES PARA IA")
        
        st.markdown("### Quando Fazer Manutenção")
        
        st.markdown("""
        **Faça manutenção quando**:
        - Estrutura dos arquivos Excel de entrada mudar
        - Novas colunas forem adicionadas aos dados
        - Novos veículos forem adicionados ao sistema
        - Lógica de rateio mudar
        - Estrutura de pastas precisar ser alterada
        
        **NÃO faça manutenção quando**:
        - Apenas dados novos forem adicionados (processe normalmente)
        - Apenas valores mudarem (processe normalmente)
        - Apenas anos novos forem processados (processe normalmente)
        """)
        
        st.markdown("### Como Fazer Manutenção Segura")
        
        st.markdown("""
        1. **Sempre teste primeiro**: Processe um ano de teste antes de processar todos
        2. **Mantenha backups**: Faça backup dos arquivos Parquet antes de modificar
        3. **Valide resultados**: Verifique se Volume, valores por veículo e histórico estão corretos
        4. **Documente mudanças**: Adicione comentários explicando alterações
        5. **Mantenha consistência**: Se alterar `tc_ext/notebooks/dados.ipynb`, altere `tc_ext/notebooks/dados_BUD.ipynb` da mesma forma
        6. **Valide merges**: Sempre verifique se chaves de merge existem antes de fazer merge
        7. **Valide tipos**: Sempre verifique tipos de dados após transformações
        """)
        
        st.markdown("### Estrutura de Dependências")
        
        st.code("""
        tc_ext/notebooks/dados.ipynb depende de:
        ├── Reporting fluxo anexo.xlsx
        │   ├── Guia "Sapiens" (dados principais)
        │   ├── Guia "Rateio" (percentuais por veículo)
        │   └── Guia "Volume" (volumes por veículo)
        └── Dados SAPIENS.xlsx
            └── Guia "Base conso" (mapeamento Custo)
        
        tc_ext/notebooks/dados_BUD.ipynb depende de:
        ├── Reporting fluxo anexo.xlsx
        │   ├── Guia "Voz de custo BDG" (dados principais)
        │   ├── Guia "Rateio BDG" (percentuais por veículo)
        │   └── Guia "Volume BDG" (volumes por veículo)
        └── Dados SAPIENS.xlsx
            └── Guia "Base conso" (mapeamento Custo)
        """, language="text")
        
        st.markdown("---")
        
        st.success("""
        **✅ Este guia contém todas as informações necessárias para fazer manutenção**
        nos notebooks de extração sem quebrar o sistema. Sempre consulte este guia
        antes de fazer alterações e siga o checklist de validação.
        """)
    
    # ==========================================
    # CAPÍTULO 2: FUNCIONAMENTO DA ATUALIZAÇÃO E EXTRAÇÃO
    # ==========================================
    
    with st.expander("🔄 **Capítulo 2: Funcionamento da Atualização e Extração**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">🔄 Capítulo 2: Funcionamento da Atualização e Extração</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Processo Completo de Atualização de Dados - Passo a Passo Detalhado
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Seção 1: Visão Geral do Processo de Atualização
        st.markdown("## 🎯 VISÃO GERAL DO PROCESSO DE ATUALIZAÇÃO {#visao-atualizacao}")
        
        st.markdown("""
        Este capítulo descreve **como funciona o processo completo de atualização de dados**,
        desde a preparação dos arquivos até a execução do processamento. Entender este fluxo
        é essencial para realizar atualizações corretamente, especialmente quando se trabalha
        com novos anos ou quando se precisa atualizar arquivos existentes.
        """)
        
        st.info("""
        **💡 Importante**: O sistema foi projetado para ser flexível e permitir atualizações
        de diferentes formas: através de upload direto na interface, colocando arquivos na
        raiz do projeto, ou organizando-os nas pastas do ano. O sistema busca automaticamente
        os arquivos na ordem de prioridade definida.
        """)
        
        st.markdown("---")
        
        # Seção 2: Ordem Cronológica dos Eventos
        st.markdown("## ⏱️ ORDEM CRONOLÓGICA DOS EVENTOS {#ordem-cronologica}")
        
        st.markdown("### Sequência Completa do Processo")
        
        with st.expander("**1️⃣ Seleção do Ano e Tipo de Extração**", expanded=False):
            st.markdown("""
            **Onde**: Página "5 - Extração de Dados" (Streamlit)
            
            **Processo**:
            1. Usuário seleciona o **ano** que deseja processar (ex: 2024, 2025, 2026)
            2. Usuário seleciona o **tipo de extração**:
               - 📊 **Dados REAIS** (tc_ext/notebooks/dados.ipynb) - Processa custos reais executados
               - 💰 **Dados BUDGET** (tc_ext/notebooks/dados_BUD.ipynb) - Processa dados de orçamento
               - 🔄 **Ambos** - Processa REAIS e BUDGET sequencialmente
            
            **Resultado**: Sistema sabe qual ano processar e quais notebooks executar
            """)
        
        with st.expander("**2️⃣ Verificação e Preparação de Arquivos**", expanded=False):
            st.markdown("""
            **Onde**: Antes do processamento, na aba "Validação de Arquivos"
            
            **Processo**:
            1. Sistema verifica se os arquivos necessários já existem
            2. Sistema mostra avisos se arquivos já existem (para evitar sobrescrita acidental)
            3. Usuário pode fazer upload de arquivos diretamente na interface
            
            **Arquivos Necessários para Dados REAIS**:
            - `Dados SAPIENS.xlsx` - Base de dados SAPIENS com classificação de custos
            - `Reporting fluxo anexo.xlsx` - Dados de custos, rateio e volumes
            
            **Arquivos Necessários para Dados BUDGET**:
            - `Dados SAPIENS.xlsx` - Base de dados SAPIENS (mesmo arquivo ou versão Budget)
            - `Reporting fluxo anexo.xlsx` - Dados de Budget (guias "Voz de custo BDG", "Rateio BDG", "Volume BDG")
            """)
        
        with st.expander("**3️⃣ Sistema de Upload de Arquivos (Opcional)**", expanded=False):
            st.markdown("""
            **Onde**: Aba "Validação de Arquivos" → Seção "📤 Upload de Arquivos"
            
            **Processo**:
            1. Usuário clica em "Browse Files" para selecionar arquivo
            2. **ANTES do upload**: Sistema verifica se arquivo já existe na pasta de destino
               - Se existe: Mostra aviso ⚠️ informando que será sobrescrito
               - Se não existe: Permite upload direto
            3. Usuário seleciona arquivo do computador
            4. **APÓS seleção**: Sistema verifica novamente se arquivo existe
               - Se existe: Mostra aviso e botão "🔄 Confirmar Sobrescrita"
               - Se não existe: Salva automaticamente
            5. Arquivo é salvo em: `dados/{ANO_SELECIONADO}/Nome_do_Arquivo.xlsx`
            6. Página recarrega automaticamente (`st.rerun()`) para atualizar status
            
            **Vantagens do Upload**:
            - Não precisa colocar arquivos na raiz do projeto
            - Arquivos são organizados automaticamente na pasta do ano
            - Sistema cria a pasta do ano automaticamente se não existir
            - Avisos preventivos evitam sobrescrita acidental
            """)
        
        with st.expander("**4️⃣ Criação da Estrutura de Pastas**", expanded=False):
            st.markdown("""
            **Onde**: Função `configurar_ano()` ou `configurar_ano_bud()` nos módulos Python
            
            **Processo** (executado automaticamente ao iniciar processamento):
            1. **Cria pasta do ano**: `dados/{ANO}/`
               - Exemplo: `dados/2024/` para ano 2024
               - Exemplo: `dados/2026/` para ano 2026 (novo ano)
            
            2. **Para dados REAIS**: Cria apenas `dados/{ANO}/`
            
            3. **Para dados BUDGET**: Cria também `dados/{ANO}/BUD/`
                    - Estrutura: `dados/2024/BUD/` para **outputs** de Budget
            
            4. **Cria pastas de histórico** (se não existirem):
               - `dados/historico_consolidado/` - Para dados REAIS
               - `dados/historico_consolidado/BUD/` - Para dados BUDGET
            
            **IMPORTANTE**: 
            - Pastas são criadas automaticamente, mesmo que não existam
            - Se a pasta já existe, não há problema (não sobrescreve)
            - Sistema usa `os.makedirs(pasta, exist_ok=True)` para criar com segurança
            """)
        
        with st.expander("**5️⃣ Sistema de Busca de Arquivos**", expanded=False):
            st.markdown("""
            **Onde**: Função `encontrar_arquivo()` nos módulos de processamento
            
            **Ordem de Prioridade de Busca** (do mais prioritário ao menos prioritário):
            
            **Para Dados REAIS**:
            1. **Primeira opção**: `dados/{ANO}/Nome_do_Arquivo.xlsx`
               - Exemplo: `dados/2024/Dados SAPIENS.xlsx`
               - **Esta é a pasta preferencial!** Arquivos aqui têm prioridade máxima
            
            2. **Segunda opção**: `./Nome_do_Arquivo.xlsx` (raiz do projeto)
               - Exemplo: `./Dados SAPIENS.xlsx`
               - Usado quando arquivo não está na pasta do ano
            
            **Para Dados BUDGET**:
            1. **Primeira opção**: `dados/{ANO}/Nome_do_Arquivo.xlsx`
               - Exemplo: `dados/2024/Dados SAPIENS.xlsx`

                2. **Segunda opção**: `./Nome_do_Arquivo.xlsx` (raiz do projeto)

                *(Compatibilidade/legado)*: se existir arquivo em `dados/{ANO}/BUD/`, ele pode ser **copiado** para `dados/{ANO}/`.
            
            **Comportamento**:
            - Sistema busca na ordem acima e usa o **primeiro arquivo encontrado**
            - Se arquivo não for encontrado em nenhum local, sistema retorna erro
            - Se arquivo for encontrado na raiz, pode ser copiado para pasta do ano (dependendo da configuração)
            
            **Exemplo Prático - Processando 2026 pela primeira vez**:
            ```
            1. Sistema cria: dados/2026/
            2. Sistema busca: dados/2026/Dados SAPIENS.xlsx → ❌ Não encontrado
            3. Sistema busca: ./Dados SAPIENS.xlsx → ✅ Encontrado na raiz
            4. Sistema usa: ./Dados SAPIENS.xlsx (da raiz)
            5. Arquivos de saída são salvos em: dados/2026/
            ```
            """)
        
        with st.expander("**6️⃣ Execução do Processamento**", expanded=False):
            st.markdown("""
            **Onde**: Aba "Executar Processamento" → Botões de execução
            
            **Processo**:
            1. Usuário clica em botão de execução:
               - "🚀 Executar tc_ext/notebooks/dados.ipynb" (para REAIS)
               - "🚀 Executar tc_ext/notebooks/dados_BUD.ipynb" (para BUDGET)
               - "🚀 Executar Ambos" (para REAIS e BUDGET)
            
            2. Sistema chama função de processamento correspondente:
               - `processar_completo()` para dados REAIS
               - `processar_completo_bud()` para dados BUDGET
            
            3. **Configuração inicial**:
               - Chama `configurar_ano()` ou `configurar_ano_bud()`
               - Cria estrutura de pastas
               - Busca arquivos usando `encontrar_arquivo()`
               - Valida se arquivos existem
            
            4. **Processamento dos dados**:
               - Lê arquivos Excel das guias corretas
               - Faz merges e transformações
               - Calcula valores por veículo
               - Processa volumes
            
            5. **Salvamento**:
               - Salva arquivos Parquet na pasta do ano (ou BUD/)
               - Salva arquivos Excel intermediários (diagnósticos)
               - Consolida histórico (concatena, não substitui)
            
            6. **Feedback ao usuário**:
               - Barra de progresso mostra status
               - Mensagens de log aparecem em tempo real
               - Mensagem de sucesso ao finalizar
            """)
        
        with st.expander("**7️⃣ Consolidação do Histórico**", expanded=False):
            st.markdown("""
            **Onde**: Função `salvar_e_consolidar()` ou `salvar_e_consolidar_bud()`
            
            **Processo**:
            1. **Carrega histórico existente** (se existir):
               - Tenta carregar: `dados/historico_consolidado/df_final_historico.parquet`
               - Se não existir, cria DataFrame vazio
            
            2. **Adiciona coluna Ano aos dados atuais**:
               - Adiciona coluna `Ano` com valor do ano processado
               - Exemplo: Se processando 2026, todos os registros recebem `Ano = 2026`
            
            3. **Concatena dados**:
               - Concatena dados do ano atual com histórico existente
               - Usa `pd.concat([historico, dados_atuais])`
            
            4. **Remove duplicatas** (se houver):
               - Verifica e remove registros duplicados
            
            5. **Valida tipos de dados**:
               - Garante que Volume é numérico (float64)
               - Converte tipos se necessário
            
            6. **Salva histórico atualizado**:
               - Salva em: `dados/historico_consolidado/df_final_historico.parquet`
               - **IMPORTANTE**: Histórico é sempre **concatenado**, nunca substituído
            
            **Resultado**: Histórico contém dados de todos os anos processados
            """)
        
        st.markdown("---")
        
        # Seção 3: Sistema de Busca de Arquivos (Detalhado)
        st.markdown("## 🔍 SISTEMA DE BUSCA DE ARQUIVOS {#busca-arquivos}")
        
        st.markdown("### Lógica de Busca Detalhada")
        
        st.markdown("""
        O sistema implementa uma **busca hierárquica** que prioriza arquivos organizados
        nas pastas do ano, mas permite flexibilidade ao buscar na raiz do projeto quando
        necessário. Isso facilita o trabalho com novos anos sem precisar mover arquivos manualmente.
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **📊 Dados REAIS - Ordem de Busca:**
            
            1. `dados/{ANO}/Dados SAPIENS.xlsx`
            2. `./Dados SAPIENS.xlsx` (raiz)
            
            1. `dados/{ANO}/Reporting fluxo anexo.xlsx`
            2. `./Reporting fluxo anexo.xlsx` (raiz)
            """)
        
        with col2:
            st.markdown("""
            **💰 Dados BUDGET - Ordem de Busca:**
            
            1. `dados/{ANO}/Dados SAPIENS.xlsx`
            2. `./Dados SAPIENS.xlsx` (raiz)
            
            1. `dados/{ANO}/Reporting fluxo anexo.xlsx`
            2. `./Reporting fluxo anexo.xlsx` (raiz)
            """)
        
        st.markdown("### Exemplos Práticos de Busca")
        
        with st.expander("**Exemplo 1: Processando 2024 (ano existente)**", expanded=False):
            st.markdown("""
            **Cenário**: Pasta `dados/2024/` já existe com arquivos
            
            **Busca de Dados SAPIENS.xlsx**:
            1. ✅ Encontrado em: `dados/2024/Dados SAPIENS.xlsx`
            2. Sistema usa este arquivo (para na primeira opção)
            
            **Resultado**: Arquivo da pasta do ano é usado (prioridade máxima)
            """)
        
        with st.expander("**Exemplo 2: Processando 2026 (ano novo)**", expanded=False):
            st.markdown("""
            **Cenário**: Pasta `dados/2026/` não existe ainda, arquivo está na raiz
            
            **Busca de Dados SAPIENS.xlsx**:
            1. ❌ Não encontrado em: `dados/2026/Dados SAPIENS.xlsx` (pasta não existe)
            2. ✅ Encontrado em: `./Dados SAPIENS.xlsx` (raiz do projeto)
            3. Sistema usa arquivo da raiz
            
            **Resultado**: 
            - Sistema cria `dados/2026/` automaticamente
            - Arquivo da raiz é usado para processamento
            - Arquivos de saída são salvos em `dados/2026/`
            - **Arquivo da raiz permanece na raiz** (não é movido automaticamente)
            """)
        
        with st.expander("**Exemplo 3: Upload de Arquivo para 2026**", expanded=False):
            st.markdown("""
            **Cenário**: Usuário faz upload de arquivo para ano 2026
            
            **Processo**:
            1. Sistema cria `dados/2026/` (se não existir)
            2. Usuário faz upload de `Dados SAPIENS.xlsx`
            3. Arquivo é salvo em: `dados/2026/Dados SAPIENS.xlsx`
            
            **Próxima busca**:
            1. ✅ Encontrado em: `dados/2026/Dados SAPIENS.xlsx`
            2. Sistema usa este arquivo (prioridade máxima)
            
            **Resultado**: Arquivo uploadado tem prioridade sobre arquivo da raiz
            """)
        
        st.markdown("---")
        
        # Seção 4: Criação de Pastas e Estrutura
        st.markdown("## 📁 CRIAÇÃO DE PASTAS E ESTRUTURA {#criacao-pastas}")
        
        st.markdown("### Estrutura Completa de Pastas")
        
        st.code("""
        dados/
        ├── 2024/                    # Ano 2024 (dados REAIS)
        │   ├── Dados SAPIENS.xlsx
        │   ├── Reporting fluxo anexo.xlsx
        │   ├── df_final.parquet
        │   ├── df_vol.parquet
        │   ├── df_ke5z_group.parquet
        │   └── BUD/                 # Dados BUDGET do ano 2024
        │       ├── Dados SAPIENS.xlsx (opcional)
        │       ├── Reporting fluxo anexo.xlsx (opcional)
        │       ├── df_final_BUD.parquet
        │       ├── df_vol_BUD.parquet
        │       └── df_ke5z_group_BUD.parquet
        │
        ├── 2025/                    # Ano 2025
        │   └── ...
        │
        ├── 2026/                    # Ano 2026 (novo ano)
        │   └── ...                  # Criado automaticamente
        │
        └── historico_consolidado/   # Histórico de todos os anos
            ├── df_final_historico.parquet
            ├── df_vol_historico.parquet
            └── BUD/
                ├── df_final_historico_BUD.parquet
                └── df_vol_historico_BUD.parquet
        """, language="text")
        
        st.markdown("### Quando as Pastas São Criadas")
        
        with st.expander("**Criação Automática**", expanded=False):
            st.markdown("""
            **Momento**: Ao iniciar o processamento (função `configurar_ano()`)
            
            **Pastas criadas automaticamente**:
            - `dados/{ANO}/` - Sempre criada, mesmo que vazia
            - `dados/{ANO}/BUD/` - Criada apenas para **outputs** do processamento BUDGET
            - `dados/historico_consolidado/` - Criada se não existir
            - `dados/historico_consolidado/BUD/` - Criada se não existir (para BUDGET)
            
            **Comando usado**: `os.makedirs(pasta, exist_ok=True)`
            - `exist_ok=True` significa que não dá erro se pasta já existe
            - Cria todas as pastas intermediárias automaticamente
            """)
        
        with st.expander("**Criação via Upload**", expanded=False):
            st.markdown("""
            **Momento**: Quando usuário faz upload de arquivo
            
            **Processo**:
            1. Usuário seleciona arquivo para upload
            2. Sistema verifica se pasta `dados/{ANO}/` existe
            3. Se não existe: Cria automaticamente com `os.makedirs(pasta_ano, exist_ok=True)`
            4. Salva arquivo em: `dados/{ANO}/Nome_do_Arquivo.xlsx`
            
            **Resultado**: Pasta do ano é criada antes mesmo do processamento
            """)
        
        st.markdown("---")
        
        # Seção 5: Sistema de Upload de Arquivos
        st.markdown("## 📤 SISTEMA DE UPLOAD DE ARQUIVOS {#sistema-upload}")
        
        st.markdown("### Funcionalidades do Upload")
        
        st.markdown("""
        O sistema de upload permite que arquivos sejam enviados diretamente pela interface
        web, sem necessidade de colocá-los manualmente na raiz do projeto ou nas pastas.
        Isso facilita especialmente o trabalho com novos anos ou atualizações de arquivos.
        """)
        
        with st.expander("**Interface de Upload**", expanded=False):
            st.markdown("""
            **Localização**: Página "5 - Extração de Dados" → Aba "Validação de Arquivos" → Seção "📤 Upload de Arquivos"
            
            **Componentes**:
            - Uploaders separados por tipo de processamento (REAIS ou BUDGET)
            - Uploaders separados por arquivo (Dados SAPIENS.xlsx e Reporting fluxo anexo.xlsx)
            - Avisos proativos mostrando se arquivo já existe
            - Mensagens de confirmação após upload bem-sucedido
            
            **Layout**: Dois uploaders lado a lado (colunas) para cada tipo de processamento
            """)
        
        with st.expander("**Fluxo Completo de Upload**", expanded=False):
            st.markdown("""
            **Passo 1: Verificação Proativa**
            - Ao carregar a página, sistema verifica se arquivos já existem
            - Se existem: Mostra aviso ⚠️ acima do botão "Browse Files"
            - Aviso informa: "O arquivo já existe e será sobrescrito se você fizer upload"
            
            **Passo 2: Seleção do Arquivo**
            - Usuário clica em "Browse Files"
            - Seleciona arquivo do computador
            - Sistema detecta que arquivo foi selecionado
            
            **Passo 3: Verificação Pós-Seleção**
            - Sistema verifica novamente se arquivo existe na pasta de destino
            - Se existe: Mostra aviso adicional e botão "🔄 Confirmar Sobrescrita"
            - Se não existe: Prossegue para salvamento automático
            
            **Passo 4: Confirmação (se necessário)**
            - Se arquivo existe, usuário deve clicar em "🔄 Confirmar Sobrescrita"
            - Botão só aparece se arquivo realmente existe
            - Confirmação evita sobrescrita acidental
            
            **Passo 5: Salvamento**
            - Arquivo é salvo em: `dados/{ANO_SELECIONADO}/Nome_do_Arquivo.xlsx`
            - Pasta do ano é criada automaticamente se não existir
            - Mensagem de sucesso é exibida
            
            **Passo 6: Atualização Automática**
            - Página recarrega automaticamente (`st.rerun()`)
            - Status dos arquivos é atualizado
            - Avisos são atualizados (se arquivo agora existe)
            """)
        
        with st.expander("**Vantagens do Sistema de Upload**", expanded=False):
            st.markdown("""
            ✅ **Organização Automática**: Arquivos são salvos na pasta correta automaticamente
            
            ✅ **Flexibilidade**: Não precisa colocar arquivos na raiz do projeto
            
            ✅ **Segurança**: Avisos preventivos evitam sobrescrita acidental
            
            ✅ **Facilidade**: Especialmente útil para novos anos (ex: 2026)
            
            ✅ **Rastreabilidade**: Mensagens claras mostram onde arquivo foi salvo
            
            ✅ **Validação**: Sistema verifica existência antes e depois do upload
            """)
        
        st.markdown("---")
        
        # Seção 6: Processamento e Execução
        st.markdown("## ⚙️ PROCESSAMENTO E EXECUÇÃO {#processamento-execucao}")
        
        st.markdown("### Fluxo de Execução Completo")
        
        st.markdown("""
        O processamento segue uma sequência bem definida, garantindo que todos os passos
        sejam executados na ordem correta e que os dados sejam processados e salvos adequadamente.
        """)
        
        with st.expander("**Fase 1: Preparação**", expanded=False):
            st.markdown("""
            1. **Configuração do Ano**:
               - Chama `configurar_ano()` ou `configurar_ano_bud()`
               - Cria estrutura de pastas
               - Define caminhos de entrada e saída
            
            2. **Busca de Arquivos**:
               - Busca `Dados SAPIENS.xlsx` na ordem de prioridade
               - Busca `Reporting fluxo anexo.xlsx` na ordem de prioridade
               - Valida se arquivos foram encontrados
            
            3. **Validação**:
               - Verifica se todos os arquivos necessários existem
               - Se faltar arquivo: Retorna erro ou aviso (dependendo da configuração)
            """)
        
        with st.expander("**Fase 2: Leitura e Transformação**", expanded=False):
            st.markdown("""
            1. **Leitura dos Dados Principais**:
               - Lê guia "Sapiens" ou "Voz de custo BDG" do Reporting fluxo anexo.xlsx
               - Cria DataFrame inicial (`df_KE5Z`)
            
            2. **Merge com Base Conso**:
               - Lê guia "Base conso" do Dados SAPIENS.xlsx
               - Faz merge adicionando coluna `Custo` (Variável/Fixo)
            
            3. **Processamento de Rateio**:
               - Lê guia "Rateio" ou "Rateio BDG"
               - Transforma colunas de meses em linhas
               - Cria DataFrame com percentuais de rateio por veículo
            
            4. **Merge e Cálculo por Veículo**:
               - Merge com dados principais
               - Calcula valores por veículo (CC21, CC22, etc.)
            
            5. **Processamento de Volume**:
               - Lê guia "Volume" ou "Volume BDG"
               - Transforma colunas de meses em linhas
               - Cria DataFrame com volumes
            
            6. **Merge Final com Volume**:
               - Adiciona coluna Volume ao DataFrame principal
            """)
        
        with st.expander("**Fase 3: Salvamento e Consolidação**", expanded=False):
            st.markdown("""
            1. **Salvamento na Pasta do Ano**:
               - Salva `df_final.parquet` em `dados/{ANO}/` (ou `BUD/`)
               - Salva `df_vol.parquet`
               - Salva `df_ke5z_group.parquet`
               - Salva arquivos Excel intermediários (diagnósticos)
            
            2. **Consolidação do Histórico**:
               - Carrega histórico existente (se houver)
               - Adiciona coluna `Ano` aos dados atuais
               - Concatena dados atuais com histórico
               - Remove duplicatas
               - Salva histórico atualizado
            
            3. **Validação Final**:
               - Verifica tipos de dados
               - Valida integridade dos arquivos salvos
            """)
        
        st.markdown("---")
        
        # Seção 7: Cenários de Uso
        st.markdown("## 📋 CENÁRIOS DE USO {#cenarios-uso}")
        
        st.markdown("### Casos Práticos Completos")
        
        with st.expander("**Cenário 1: Primeira Vez Processando um Novo Ano (ex: 2026)**", expanded=False):
            st.markdown("""
            **Situação**: Nunca processou dados de 2026, arquivos estão na raiz do projeto
            
            **Passo a Passo**:
            
            1. **Acessar página de extração**:
               - Selecionar ano: 2026
               - Selecionar tipo: "📊 Dados REAIS" ou "🔄 Ambos"
            
            2. **Opção A - Usar Upload** (Recomendado):
               - Ir para aba "Validação de Arquivos"
               - Fazer upload de `Dados SAPIENS.xlsx` → Salvo em `dados/2026/`
               - Fazer upload de `Reporting fluxo anexo.xlsx` → Salvo em `dados/2026/`
            
            3. **Opção B - Usar Arquivos da Raiz**:
               - Colocar arquivos na raiz do projeto
               - Sistema buscará automaticamente na raiz se não encontrar na pasta do ano
            
            4. **Executar processamento**:
               - Clicar em "🚀 Executar tc_ext/notebooks/dados.ipynb"
               - Sistema cria `dados/2026/` automaticamente
               - Sistema busca arquivos (encontra na raiz ou na pasta do ano)
               - Processa e salva em `dados/2026/`
               - Consolida histórico
            
            **Resultado**: 
            - Pasta `dados/2026/` criada com arquivos processados
            - Histórico atualizado com dados de 2026
            """)
        
        with st.expander("**Cenário 2: Atualizar Arquivos de um Ano Existente**", expanded=False):
            st.markdown("""
            **Situação**: Já processou 2024 antes, mas recebeu arquivos atualizados
            
            **Passo a Passo**:
            
            1. **Acessar página de extração**:
               - Selecionar ano: 2024
               - Selecionar tipo: "📊 Dados REAIS"
            
            2. **Verificar arquivos existentes**:
               - Sistema mostra aviso: "⚠️ O arquivo já existe"
               - Aviso aparece antes mesmo de fazer upload
            
            3. **Fazer upload do arquivo atualizado**:
               - Selecionar arquivo atualizado
               - Sistema mostra aviso: "Arquivo será sobrescrito"
               - Clicar em "🔄 Confirmar Sobrescrita"
               - Arquivo é salvo substituindo o anterior
            
            4. **Executar processamento**:
               - Clicar em "🚀 Executar tc_ext/notebooks/dados.ipynb"
               - Sistema usa arquivo atualizado de `dados/2024/`
               - Processa e atualiza arquivos Parquet
               - Atualiza histórico (concatena, não substitui)
            
            **Resultado**: 
            - Arquivos de 2024 atualizados
            - Histórico contém versão mais recente
            """)
        
        with st.expander("**Cenário 3: Processar Ambos (REAIS e BUDGET) para Novo Ano**", expanded=False):
            st.markdown("""
            **Situação**: Processar dados REAIS e BUDGET de 2026 pela primeira vez
            
            **Passo a Passo**:
            
            1. **Preparar arquivos REAIS**:
               - Upload de `Dados SAPIENS.xlsx` (REAIS) → `dados/2026/`
               - Upload de `Reporting fluxo anexo.xlsx` (REAIS) → `dados/2026/`
            
            2. **Preparar arquivos BUDGET** (se diferentes):
               - Upload de `Dados SAPIENS.xlsx` (BUD) → `dados/2026/` (mesmo arquivo ou versão BUD)
               - Upload de `Reporting fluxo anexo.xlsx` (BUD) → `dados/2026/` (com guias BDG)
            
            3. **Executar processamento**:
               - Selecionar tipo: "🔄 Ambos"
               - Clicar em "🚀 Executar Ambos"
               - Sistema processa REAIS primeiro → Salva em `dados/2026/`
               - Sistema processa BUDGET depois → Salva em `dados/2026/BUD/`
               - Consolida ambos os históricos
            
            **Resultado**: 
            - Estrutura completa criada: `dados/2026/` e `dados/2026/BUD/`
            - Históricos REAIS e BUDGET atualizados
            """)
        
        with st.expander("**Cenário 4: Processar Apenas BUDGET para Ano Existente**", expanded=False):
            st.markdown("""
            **Situação**: Já processou REAIS de 2024, agora quer processar BUDGET
            
            **Passo a Passo**:
            
            1. **Preparar arquivos BUDGET**:
               - Upload de `Dados SAPIENS.xlsx` (BUD) → `dados/2024/`
               - Upload de `Reporting fluxo anexo.xlsx` (BUD) → `dados/2024/`
            
            2. **Executar processamento BUDGET**:
               - Selecionar tipo: "💰 Dados BUDGET"
               - Clicar em "🚀 Executar tc_ext/notebooks/dados_BUD.ipynb"
               - Sistema cria `dados/2024/BUD/` automaticamente
               - Processa e salva em `dados/2024/BUD/`
               - Consolida histórico BUDGET
            
            **Resultado**: 
            - Pasta `dados/2024/BUD/` criada com dados de Budget
            - Histórico BUDGET atualizado
            - Dados REAIS permanecem inalterados
            """)
        
        st.markdown("---")
        
        st.success("""
        **✅ Este capítulo descreve completamente o funcionamento do sistema de atualização**
        e extração de dados. Use estas informações para realizar atualizações de forma
        segura e eficiente, especialmente ao trabalhar com novos anos ou atualizar
        arquivos existentes.
        """)


# ==========================================
# TC VEÍCULOS: GUIA DE BEST ESTIMATE
# ==========================================
elif indice_selecionado == "🔮 Guia de Best Estimate" and modulo_doc == "🚗 TC Veículos":
    st.header("🔮 Guia de Best Estimate — TC Veículos")

    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
    <h2 style="color: white; margin: 0;">🔮 Best Estimate — TC Veículos</h2>
    <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Simulador de premissas e análise de Forecast para o módulo TC Veículos
    </p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("📋 **Visão Geral do Best Estimate**", expanded=True):
        st.markdown("""
        ### 🔮 O que é o Best Estimate?

        O Best Estimate (BE) no TC Veículos projeta custos futuros com base na **média histórica**
        dos meses já realizados, ajustada por premissas de **sensibilidade**, **inflação** e **volume**.

        O sistema é dividido em duas páginas:
        - **Simulador** (`2 - Best Estimate - Simulador.py`): onde o usuário configura premissas e gera o Forecast
        - **Análise** (`best_estimate_analise_tc.py`): dashboard que exibe Real + Forecast no mesmo layout da Home

        ### 📂 Dados Gerados

        | Arquivo | Descrição |
        |---------|-----------|
        | `dados/TC_Principal/Forecast/forecast_completo.parquet` | Projeção mês a mês com coluna `Tipo = 'BE'` |
        | `dados/TC_Principal/Forecast/premissas.json` | Premissas aplicadas |
        """)

    with st.expander("⚙️ **Simulador — Premissas**", expanded=False):
        st.markdown("""
        ### 🎛️ Configuração de Premissas

        O simulador permite configurar os seguintes parâmetros:

        | Premissa | Escopo | Efeito |
        |----------|--------|--------|
        | **Sensibilidade** | Por oficina (Type 06) ou global | Controla o quanto a variação de volume afeta o custo |
        | **Inflação** | Por Type 06 ou global | Aplica % de reajuste sobre **todos** os custos (fixos e variáveis) |
        | **Volume** | Por veículo | Volume de produção projetado para o mês futuro |

        ### 📐 Fórmula Geral (linha a linha)

        ```
        BE = Média_Histórica × Fator_Variação × Fator_Inflação
        ```

        **Onde:**
        - `Fator_Variação` = 1 + (Variação_Volume × Sensibilidade)
        - `Fator_Inflação` = 1 + (Inflação / 100)
        - `Variação_Volume` = (Volume_Mês_Futuro / Volume_Médio_Histórico) − 1

        **Aplicação por tipo de custo:**

        | Tipo | Sensibilidade | Fórmula resultante |
        |------|---------------|--------------------|
        | **Fixo** | 0% | `BE = Média_Histórica × 1,0 × (1 + Inflação%)` — sem ajuste de volume |
        | **Variável** | 100% | `BE = Média_Histórica × (Vol_Futuro / Vol_Histórico) × (1 + Inflação%)` |
        | **Semi-variável** | 0% < s < 100% | `BE = Média_Histórica × (1 + Var_Volume × s) × (1 + Inflação%)` |

        **Exemplo numérico:**
        ```
        Custo médio histórico: R$ 10.000
        Volume histórico médio: 1.000 un | Volume futuro: 1.100 un
        Sensibilidade: 50% | Inflação: 5%

        Passo 1 — Variação de volume: 1.100 / 1.000 − 1 = +10%
        Passo 2 — Ajuste por sensibilidade: 10% × 50% = 5%
        Passo 3 — Fator de variação: 1 + 0,05 = 1,05
        Passo 4 — Fator de inflação: 1 + 0,05 = 1,05
        Passo 5 — BE = 10.000 × 1,05 × 1,05 = R$ 11.025
        ```

        - **CPU BE** = Custo BE Total / Volume Projetado

        ### ⚠️ Regras Especiais

        - Quando `chaves_volume_base = []` (custo sem dimensão Veículo), o sistema calcula
          a média de volume diretamente sem `groupby`
        - Para custos com Veículo, o volume é somado por grupo (`.sum()`)
        - A inflação é aplicada **após** o ajuste por sensibilidade
        - Sensibilidade por Type 06 sobrescreve a sensibilidade global (Fixo/Variável)
        """)

    with st.expander("📊 **Análise — Dashboard de Forecast**", expanded=False):
        st.markdown("""
        ### 📈 Layout da Análise BE

        A página de Análise reutiliza o layout da Home TC Veículos, mas alimentada
        pelos dados de `forecast_completo.parquet`:

        - **KPIs**: Custo FP Real vs BE, com deltas e percentuais
        - **Gráficos por período**: barras com diferenciação visual:
          - 🟣 **Roxo escuro** (`#4C1D95`): meses Históricos (realizados)
          - 🟣 **Roxo claro** (`#C4B5FD`): meses de Best Estimate (projetados)
        - **Tabelas**: Análise Flex com dados reais + projetados

        ### 🔄 Fluxo de Atualização

        ```
        Simulador → gera forecast_completo.parquet
            ↓
        Análise BE → lê forecast + real
            ↓
        Unifica com coluna Tipo (Histórico / BE)
            ↓
        Exibe em gráficos e tabelas
        ```
        """)

    with st.expander("🚗 **Rateio BE por Veículo**", expanded=False):
        st.markdown("""
        ### 📊 Função `ratear_be_por_veiculo()`

        Quando dados de BE não possuem a coluna `Veículo`, é necessário distribuir
        o custo proporcionalmente usando os mesmos percentuais do Real:

        ```
        CustoFP_Veículo_BE = CustoFP_BE × Percentual(Veículo, Oficina)
        ```

        **Parâmetros:**
        - `df_be`: DataFrame com dados do Best Estimate
        - `df_percentual`: DataFrame com [Oficina, Veículo, Período, Percentual]
        - `col_custo`: coluna a ratear (default: 'Custo FP')

        **Tratamento de oficinas sem rateio:**
        - Se uma oficina não tem percentual definido, o custo é dividido igualmente
          entre todos os veículos conhecidos
        """)

    with st.expander("📥 **Integração com Waterfall**", expanded=False):
        st.markdown("""
        ### 🌊 BE no Waterfall

        O Waterfall do TC Veículos integra dados de Best Estimate para meses futuros:

        1. Carrega `forecast_completo.parquet` via `load_forecast_completo()`
        2. Identifica meses de BE que **não existem** nos dados reais
        3. Concatena dados reais + BE com coluna `Fonte` ('Real' ou 'BE')
        4. Aplica rateio por veículo via `ratear_be_por_veiculo()` quando necessário

        **Cores no gráfico Waterfall:**
        - Barras de meses BE usam cores diferenciadas (BE) para distinguir do Real
        - A coluna `Fonte` permite filtrar/identificar a origem dos dados
        """)

    with st.expander("🔧 **Pipeline Técnico**", expanded=False):
        st.markdown("""
        ### 📂 Arquivos Envolvidos

        | Arquivo | Função |
        |---------|--------|
        | `pages/2 - Best Estimate - Simulador.py` | Página do simulador (tab Real + tab Budget) |
        | `tc_principal/pages/best_estimate_simulador_tc.py` | Lógica do simulador TC Veículos |
        | `tc_principal/pages/best_estimate_analise_tc.py` | Dashboard de análise BE |
        | `tc_principal/shared.py` | `ratear_be_por_veiculo()`, loaders de forecast |

        ### 💾 Armazenamento

        ```
        dados/TC_Principal/Forecast/
        ├── forecast_completo.parquet
        └── premissas.json
        ```
        """)

# ==========================================
# SEÇÃO 5: GUIA DE BEST ESTIMATE
# ==========================================
elif indice_selecionado == "🔮 Guia de Best Estimate":
    st.header("🔮 Guia de Best Estimate — TC Extendido")
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
    <h2 style="color: white; margin: 0;">🔮 Documentação Completa do Best Estimate</h2>
    <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Teoria, Cálculos, Estrutura e Funcionamento do Sistema de Previsão
    </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Índice interno
    st.markdown("## 📋 Índice do Guia")
    st.markdown("""
    ### 📖 Capítulo 1: Teoria e Funcionamento do Best Estimate
    1. [O que é Best Estimate?](#o-que-e-best-estimate)
    2. [Teoria e Conceitos Fundamentais](#teoria-conceitos)
    3. [Cálculo de Médias Históricas](#calculo-medias)
    4. [Sensibilidade e Inflação](#sensibilidade-inflacao)
    5. [Fórmulas e Lógica de Cálculo](#formulas-logica)
    6. [Tipos de Custos: Fixo vs Variável](#tipos-custos)
    7. [Volume e Proporções](#volume-proporcoes)
    
    ### 🔄 Capítulo 2: Estrutura, Atualização e Páginas
    1. [Estrutura de Pastas do Forecast](#estrutura-forecast)
    2. [Ordem Cronológica de Atualização](#ordem-cronologica-forecast)
    3. [Página 2 - Best Estimate Simulador](#pagina-simulador)
    4. [Página - Best Estimate (Análise)](#pagina-analise)
    5. [Fluxo de Dados e Processamento](#fluxo-dados-forecast)
    6. [Arquivos Gerados](#arquivos-gerados-forecast)
    7. [Cenários de Uso](#cenarios-uso-forecast)
    """)
    
    st.markdown("---")
    
    # ==========================================
    # CAPÍTULO 1: TEORIA E FUNCIONAMENTO DO BEST ESTIMATE
    # ==========================================
    
    with st.expander("📖 **Capítulo 1: Teoria e Funcionamento do Best Estimate**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">📖 Capítulo 1: Teoria e Funcionamento do Best Estimate</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Conceitos, Teoria e Cálculos do Sistema de Previsão de Custos
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Seção 1: O que é Best Estimate?
        st.markdown("## 🎯 O QUE É BEST ESTIMATE? {#o-que-e-best-estimate}")
        
        st.markdown("""
        ### Definição e Conceito
        
        **Best Estimate** (Melhor Estimativa) é uma metodologia de previsão de custos que combina:
        - **Dados históricos** (médias de períodos anteriores)
        - **Ajustes por sensibilidade** (resposta a variações de volume)
        - **Ajustes por inflação** (correção monetária)
        - **Classificação de custos** (Fixo vs Variável)
        
        **Objetivo Principal:**
        Prever os custos futuros com base em padrões históricos, ajustados para refletir mudanças esperadas
        em volume de produção e inflação, permitindo planejamento financeiro mais preciso.
        
        **Aplicação no Sistema TC:**
        O Best Estimate é usado para gerar previsões de custos para períodos futuros, permitindo comparações
        entre o que foi planejado (Budget), o que realmente aconteceu (Real) e o que se espera que aconteça
        (Best Estimate/Forecast).
        """)
        
        st.info("""
        **💡 Importante**: Best Estimate não é uma simples projeção linear. Ele considera a natureza dos custos
        (fixos ou variáveis) e aplica sensibilidades diferentes para cada tipo, resultando em previsões mais
        realistas e acuradas.
        """)
        
        st.markdown("---")
        
        # Seção 2: Teoria e Conceitos Fundamentais
        st.markdown("## 📚 TEORIA E CONCEITOS FUNDAMENTAIS {#teoria-conceitos}")
        
        st.markdown("""
        ### Fundamentos Teóricos
        
        **1. Princípio da Média Histórica:**
        - O Best Estimate parte do pressuposto de que o comportamento histórico é um bom indicador do futuro
        - Médias calculadas sobre períodos selecionados fornecem uma base sólida para previsões
        - Períodos anômalos podem ser excluídos para melhorar a acurácia
        
        **2. Princípio da Sensibilidade:**
        - Custos **fixos** não variam com volume (sensibilidade = 0%)
        - Custos **variáveis** variam proporcionalmente ao volume (sensibilidade = 100%)
        - Sensibilidades intermediárias (0% < sensibilidade < 100%) representam custos semi-variáveis
        
        **3. Princípio da Inflação:**
        - Inflação afeta todos os custos de forma uniforme
        - É aplicada como um fator multiplicador sobre o custo ajustado por sensibilidade
        - Permite correção monetária para períodos futuros
        
        **4. Princípio da Proporcionalidade de Volume:**
        - A variação de volume impacta diferentemente custos fixos e variáveis
        - Custos fixos são "diluídos" quando o volume aumenta (CPU diminui)
        - Custos variáveis aumentam proporcionalmente ao volume
        """)
        
        st.markdown("---")
        
        # Seção 3: Cálculo de Médias Históricas
        st.markdown("## 📊 CÁLCULO DE MÉDIAS HISTÓRICAS {#calculo-medias}")
        
        st.markdown("""
        ### Processo de Cálculo de Médias
        
        **Passo 1: Seleção de Períodos**
        - O usuário seleciona quais períodos históricos serão usados para calcular a média
        - Exemplo: Janeiro 2024, Fevereiro 2024, Março 2024
        - Períodos podem ser excluídos se forem considerados anômalos
        
        **Passo 2: Filtragem de Dados**
        - Aplicam-se os mesmos filtros usados na análise (Oficina, Veículo, Type 05, Type 06, etc.)
        - Garante que a média seja calculada sobre o mesmo contexto operacional
        
        **Passo 3: Agrupamento e Agregação**
        - Dados são agrupados por chaves únicas: `Oficina`, `Veículo`, `Tipo_Custo`, `Type 06`, etc.
        - Para cada grupo, calcula-se a média dos valores históricos
        - Fórmula: `Média_Histórica = Σ(Valores_Históricos) / Número_de_Períodos`
        
        **Passo 4: Volume Médio Histórico**
        - Calcula-se também o volume médio histórico para os mesmos períodos
        - Usado para calcular proporções de volume futuro vs histórico
        - Fórmula: `Volume_Médio_Histórico = Σ(Volumes_Históricos) / Número_de_Períodos`
        
        **Exemplo Prático:**
        ```
        Períodos selecionados: Janeiro 2024, Fevereiro 2024, Março 2024
        
        Para Oficina A, Veículo CC21, Type 06 "Material":
        - Janeiro 2024: R$ 10.000
        - Fevereiro 2024: R$ 12.000
        - Março 2024: R$ 11.000
        
        Média Histórica = (10.000 + 12.000 + 11.000) / 3 = R$ 11.000
        ```
        """)
        
        with st.expander("**🔍 Detalhes Técnicos do Cálculo de Médias**", expanded=False):
            st.markdown("""
            **Agrupamento por Chaves:**
            - O sistema agrupa dados por múltiplas dimensões simultaneamente
            - Chaves padrão: `['Oficina', 'Veículo', 'Tipo_Custo', 'Type 06', ...]`
            - Cada combinação única de chaves gera uma linha no forecast
            
            **Tratamento de Dados Faltantes:**
            - Se um período não tiver dados para uma combinação de chaves, ele é excluído do cálculo
            - A média é calculada apenas sobre períodos com dados disponíveis
            - Isso evita distorções por períodos incompletos
            
            **Normalização de Períodos:**
            - Períodos são normalizados para comparação (ex: "Janeiro 2024" → "janeiro 2024")
            - Permite comparação case-insensitive e tolerante a espaços
            - Anos são extraídos dos períodos para filtragem adicional
            """)
        
        st.markdown("---")
        
        # Seção 4: Sensibilidade e Inflação
        st.markdown("## ⚙️ SENSIBILIDADE E INFLAÇÃO {#sensibilidade-inflacao}")
        
        st.markdown("""
        ### Sensibilidade ao Volume
        
        **Conceito:**
        Sensibilidade mede o quanto um custo responde a variações no volume de produção.
        
        **Tipos de Sensibilidade:**
        
        **1. Sensibilidade Fixa (0%):**
        - Aplicada a custos **fixos**
        - Independente da variação de volume, o custo permanece constante
        - Exemplos: Aluguel, salários fixos, depreciação
        - Fórmula: `Custo_Ajustado = Custo_Original` (sem alteração)
        
        **2. Sensibilidade Variável (100%):**
        - Aplicada a custos **variáveis**
        - Varia proporcionalmente ao volume
        - Se volume aumenta 10%, custo aumenta 10%
        - Exemplos: Matéria-prima, energia variável, comissões
        - Fórmula: `Custo_Ajustado = Custo_Original * (Volume_Novo / Volume_Histórico)`
        
        **3. Sensibilidades Intermediárias (0% < sensibilidade < 100%):**
        - Aplicadas a custos **semi-variáveis**
        - Resposta parcial a variações de volume
        - Exemplo: Se sensibilidade = 50% e volume aumenta 10%, custo aumenta 5%
        - Fórmula: `Custo_Ajustado = Custo_Original * (1 + (Variação_Volume * Sensibilidade))`
        
        **4. Sensibilidade por Type 06:**
        - Cada Type 06 pode ter sua própria sensibilidade específica
        - Permite ajustes finos por categoria de custo
        - Sobrescreve a sensibilidade geral (Fixo/Variável) quando configurada
        """)
        
        st.markdown("""
        ### Inflação
        
        **Conceito:**
        Inflação é aplicada como um ajuste monetário uniforme sobre todos os custos, independente
        de serem fixos ou variáveis.
        
        **Aplicação:**
        - Inflação é configurada como percentual (ex: 5% ao ano)
        - É aplicada após o ajuste por sensibilidade
        - Fórmula: `Custo_Final = Custo_Ajustado_Sensibilidade * (1 + Inflação/100)`
        
        **Exemplo:**
        ```
        Custo médio histórico: R$ 10.000
        Variação de volume: +10%
        Sensibilidade: 50%
        Inflação: 5%
        
        Passo 1: Ajuste por sensibilidade
        Variação_ajustada = 10% * 50% = 5%
        Custo_ajustado = 10.000 * (1 + 0.05) = R$ 10.500
        
        Passo 2: Aplicar inflação
        Custo_final = 10.500 * (1 + 0.05) = R$ 11.025
        ```
        """)
        
        st.markdown("---")
        
        # Seção 5: Fórmulas e Lógica de Cálculo
        st.markdown("## 🧮 FÓRMULAS E LÓGICA DE CÁLCULO {#formulas-logica}")
        
        st.markdown("""
        ### Fórmula Completa do Best Estimate
        
        **Fórmula Geral (linha a linha):**
        ```
        Best_Estimate = Média_Histórica * Fator_Variação * Fator_Inflação
        ```
        
        **Onde:**
        - `Média_Histórica` = Média dos custos históricos para a combinação de chaves
        - `Fator_Variação` = 1 + (Variação_Percentual_Volume * Sensibilidade)
        - `Fator_Inflação` = 1 + (Inflação / 100)
        
        **Cálculo Detalhado Passo a Passo:**
        
        **1. Calcular Proporção de Volume:**
        ```
        proporção_volume = Volume_do_Mês_Futuro / Volume_Médio_Histórico
        ```
        
        **2. Calcular Variação Percentual:**
        ```
        variação_percentual = proporção_volume - 1.0
        ```
        - Se `variação_percentual > 0`: Volume aumentou
        - Se `variação_percentual < 0`: Volume diminuiu
        - Se `variação_percentual = 0`: Volume permaneceu igual
        
        **3. Aplicar Sensibilidade:**
        ```
        variação_ajustada = variação_percentual * sensibilidade
        ```
        - Para custos fixos: `sensibilidade = 0` → `variação_ajustada = 0`
        - Para custos variáveis: `sensibilidade = 1.0` → `variação_ajustada = variação_percentual`
        
        **4. Calcular Fator de Variação:**
        ```
        fator_variação = 1.0 + variação_ajustada
        ```
        
        **5. Calcular Fator de Inflação:**
        ```
        fator_inflação = 1.0 + (inflação / 100.0)
        ```
        
        **6. Calcular Best Estimate Final:**
        ```
        Best_Estimate = Média_Histórica * fator_variação * fator_inflação
        ```
        """)
        
        with st.expander("**📐 Exemplo Completo de Cálculo**", expanded=False):
            st.markdown("""
            **Cenário:**
            - Média histórica: R$ 10.000
            - Volume médio histórico: 1.000 unidades
            - Volume do mês futuro: 1.100 unidades
            - Tipo de custo: Variável (sensibilidade = 100%)
            - Inflação: 5%
            
            **Cálculo:**
            
            **Passo 1:** Proporção de volume
            ```
            proporção = 1.100 / 1.000 = 1.1
            ```
            
            **Passo 2:** Variação percentual
            ```
            variação = 1.1 - 1.0 = 0.1 (10% de aumento)
            ```
            
            **Passo 3:** Aplicar sensibilidade
            ```
            variação_ajustada = 0.1 * 1.0 = 0.1 (10%)
            ```
            
            **Passo 4:** Fator de variação
            ```
            fator_variação = 1.0 + 0.1 = 1.1
            ```
            
            **Passo 5:** Fator de inflação
            ```
            fator_inflação = 1.0 + (5/100) = 1.05
            ```
            
            **Passo 6:** Best Estimate
            ```
            Best_Estimate = 10.000 * 1.1 * 1.05 = R$ 11.550
            ```
            
            **Interpretação:**
            O custo previsto é R$ 11.550, representando:
            - Aumento de 10% devido ao aumento de volume (de 1.000 para 1.100 unidades)
            - Aumento adicional de 5% devido à inflação
            - Total: 15.5% de aumento sobre a média histórica
            """)
        
        st.markdown("---")
        
        # Seção 6: Tipos de Custos
        st.markdown("## 💰 TIPOS DE CUSTOS: FIXO VS VARIÁVEL {#tipos-custos}")
        
        st.markdown("""
        ### Classificação de Custos
        
        **Custos Fixos:**
        - **Características:** Não variam com o volume de produção
        - **Sensibilidade:** 0% (zero por cento)
        - **Exemplos:** Aluguel, salários fixos, depreciação, seguros
        - **Comportamento no Best Estimate:**
          - Média histórica é mantida (sem ajuste por volume)
          - Apenas inflação é aplicada
          - Fórmula: `Best_Estimate_Fixo = Média_Histórica_Fixo * (1 + Inflação/100)`
        
        **Custos Variáveis:**
        - **Características:** Variam proporcionalmente ao volume de produção
        - **Sensibilidade:** 100% (cem por cento)
        - **Exemplos:** Matéria-prima, energia variável, comissões, peças de reposição
        - **Comportamento no Best Estimate:**
          - Média histórica é ajustada pela proporção de volume
          - Inflação é aplicada sobre o valor ajustado
          - Fórmula: `Best_Estimate_Variável = Média_Histórica_Variável * (Volume_Futuro/Volume_Histórico) * (1 + Inflação/100)`
        
        **Identificação no Sistema:**
        - A coluna `Custo` (ou `Tipo_Custo`) contém os valores: `'Fixo'` ou `'Variável'`
        - Esta classificação vem do merge com a Base Conso (Dados SAPIENS.xlsx)
        - Cada linha de dados deve ter esta classificação para o cálculo correto
        """)
        
        st.markdown("---")
        
        # Seção 7: Volume e Proporções
        st.markdown("## 📈 VOLUME E PROPORÇÕES {#volume-proporcoes}")
        
        st.markdown("""
        ### Importância do Volume no Best Estimate
        
        **Volume como Base de Cálculo:**
        - O volume futuro é usado para calcular a proporção em relação ao volume histórico
        - Esta proporção determina o ajuste aplicado aos custos variáveis
        - Volume médio histórico é calculado sobre os mesmos períodos usados para a média de custos
        
        **Cálculo de Proporção:**
        ```
        proporção = Volume_Mês_Futuro / Volume_Médio_Histórico
        ```
        
        **Interpretação da Proporção:**
        - `proporção > 1.0`: Volume futuro é maior que o histórico → Custos variáveis aumentam
        - `proporção < 1.0`: Volume futuro é menor que o histórico → Custos variáveis diminuem
        - `proporção = 1.0`: Volume futuro igual ao histórico → Sem ajuste por volume (apenas inflação)
        
        **Impacto nos Custos:**
        - **Custos Fixos:** Não são afetados pela proporção (sensibilidade = 0%)
        - **Custos Variáveis:** São multiplicados pela proporção (sensibilidade = 100%)
        - **Custos Semi-Variáveis:** São multiplicados por `1 + (proporção - 1) * sensibilidade`
        """)
        
        st.success("""
        **✅ Este capítulo descreve completamente a teoria e funcionamento do Best Estimate.**
        Use estas informações para entender como as previsões são calculadas e como os parâmetros
        (sensibilidade, inflação, períodos históricos) impactam os resultados.
        """)
    
    # ==========================================
    # CAPÍTULO 2: ESTRUTURA, ATUALIZAÇÃO E PÁGINAS
    # ==========================================
    
    with st.expander("🔄 **Capítulo 2: Estrutura, Atualização e Páginas**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">🔄 Capítulo 2: Estrutura, Atualização e Páginas</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Estrutura de Pastas, Ordem de Atualização e Funcionalidades das Páginas
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Seção 1: Estrutura de Pastas do Forecast
        st.markdown("## 📁 ESTRUTURA DE PASTAS DO FORECAST {#estrutura-forecast}")
        
        st.markdown("""
        ### Organização da Pasta `dados/Forecast/`
        
        A pasta `Forecast/` é criada automaticamente quando o Best Estimate é gerado e contém
        os arquivos de previsão calculados pelo sistema.
        
        **Estrutura Completa:**
        ```
        dados/
        └── Forecast/                       # 🔮 Dados de Best Estimate/Forecast
            ├── forecast_completo.parquet   # Forecast completo com todas as linhas
            ├── forecast_historico.parquet  # Histórico de forecasts gerados
            ├── forecast_previsao.parquet   # Previsões futuras
            ├── df_final_historico_forecast.parquet  # Dados históricos filtrados para forecast
            └── df_vol_historico.parquet    # Volumes históricos para cálculo
        ```
        
        **Características:**
        - **Criação Automática:** A pasta é criada automaticamente se não existir
        - **Substituição:** Arquivos são substituídos a cada geração (não concatenados)
        - **Prioridade:** Sistema busca arquivos nesta pasta primeiro antes de usar histórico consolidado
        - **Formato:** Todos os arquivos são Parquet para performance otimizada
        """)
        
        st.markdown("---")
        
        # Seção 2: Ordem Cronológica de Atualização
        st.markdown("## ⏱️ ORDEM CRONOLÓGICA DE ATUALIZAÇÃO {#ordem-cronologica-forecast}")
        
        st.markdown("### Sequência Completa do Processo")
        
        with st.expander("**1️⃣ Configuração de Parâmetros**", expanded=False):
            st.markdown("""
            **Onde**: Página 2 (Simulador) ou Página 3 (Análise)
            
            **Processo**:
            1. Usuário seleciona **períodos históricos** para calcular a média
               - Exemplo: Janeiro 2024, Fevereiro 2024, Março 2024
               - Períodos podem ser excluídos se anômalos
            
            2. Usuário configura **sensibilidades**:
               - Sensibilidade para custos fixos (geralmente 0%)
               - Sensibilidade para custos variáveis (geralmente 100%)
               - Sensibilidades específicas por Type 06 (opcional)
            
            3. Usuário configura **inflação**:
               - Percentual de inflação anual (ex: 5%)
               - Pode ser aplicada globalmente ou por Type 06
            
            4. Usuário seleciona **períodos futuros** para forecast:
               - Exemplo: Abril 2024, Maio 2024, Junho 2024
               - Volumes futuros são informados ou calculados
            
            **Resultado**: Sistema tem todos os parâmetros necessários para calcular o forecast
            """)
        
        with st.expander("**2️⃣ Carregamento de Dados Históricos**", expanded=False):
            st.markdown("""
            **Onde**: Função `load_data()` nas páginas 2 e 3
            
            **Ordem de Prioridade de Busca**:
            1. **Primeira opção**: `dados/Forecast/forecast_completo.parquet`
               - Se existir, pode ser usado como base (mas forecast é recalculado)
            
            2. **Segunda opção**: `dados/historico_consolidado/df_final_historico.parquet`
               - **Fonte principal** de dados históricos
               - Contém todos os anos consolidados
            
            3. **Terceira opção**: `dados/{ANO}/df_final.parquet`
               - Dados específicos do ano (se filtro de ano aplicado)
            
            **Processo**:
            - Sistema carrega dados históricos completos
            - Aplica filtros selecionados (Oficina, Veículo, Type 05, Type 06, etc.)
            - Filtra pelos períodos selecionados para cálculo de média
            - Remove períodos excluídos (meses_excluir_media)
            """)
        
        with st.expander("**3️⃣ Cálculo de Médias Históricas**", expanded=False):
            st.markdown("""
            **Onde**: Função `calcular_medias_forecast()` nas páginas 2 e 3
            
            **Processo**:
            1. **Filtrar dados pelos períodos selecionados**:
               - Apenas períodos marcados para média são considerados
               - Períodos excluídos são removidos
            
            2. **Agrupar por chaves únicas**:
               - Chaves: `['Oficina', 'Veículo', 'Tipo_Custo', 'Type 06', ...]`
               - Cada combinação única gera uma linha no forecast
            
            3. **Calcular média por grupo**:
               - Soma dos valores históricos / número de períodos
               - Usa coluna `Total` (nunca `Valor`)
            
            4. **Calcular volume médio histórico**:
               - Mesma lógica: agrupa e calcula média de volumes
               - Usado para calcular proporções futuras
            
            **Resultado**: DataFrame com médias históricas por combinação de chaves
            """)
        
        with st.expander("**4️⃣ Cálculo do Forecast**", expanded=False):
            st.markdown("""
            **Onde**: Função `calcular_forecast_completo()` nas páginas 2 e 3
            
            **Processo (linha a linha)**:
            1. **Para cada linha do forecast**:
               - Obtém média histórica da combinação de chaves
               - Obtém volume do mês futuro
               - Obtém volume médio histórico
            
            2. **Calcula proporção de volume**:
               ```
               proporção = Volume_Mês_Futuro / Volume_Médio_Histórico
               ```
            
            3. **Calcula variação percentual**:
               ```
               variação = proporção - 1.0
               ```
            
            4. **Aplica sensibilidade**:
               - Se `Tipo_Custo == 'Fixo'`: usa `sensibilidade_fixo`
               - Se `Tipo_Custo == 'Variável'`: usa `sensibilidade_variavel`
               - Se modo Type 06: usa sensibilidade específica do Type 06
               ```
               variação_ajustada = variação * sensibilidade
               ```
            
            5. **Calcula forecast**:
               ```
               fator_variação = 1.0 + variação_ajustada
               fator_inflação = 1.0 + (inflação / 100.0)
               forecast = Média_Histórica * fator_variação * fator_inflação
               ```
            
            **Resultado**: DataFrame completo com forecast linha a linha
            """)
        
        with st.expander("**5️⃣ Salvamento dos Arquivos**", expanded=False):
            st.markdown("""
            **Onde**: Função de salvamento nas páginas 2 e 3
            
            **Processo**:
            1. **Verificar/Criar pasta Forecast**:
               - Verifica se `dados/Forecast/` existe
               - Se não existe, cria automaticamente: `os.makedirs(pasta_forecast, exist_ok=True)`
            
            2. **Salvar forecast_completo.parquet**:
               - Arquivo principal com todas as linhas do forecast
               - Substitui arquivo anterior (não concatena)
               - Localização: `dados/Forecast/forecast_completo.parquet`
            
            3. **Salvar forecast_historico.parquet** (se aplicável):
               - Histórico de forecasts gerados anteriormente
               - Pode ser concatenado com novo forecast
            
            4. **Salvar forecast_previsao.parquet** (se aplicável):
               - Apenas previsões futuras (sem dados históricos)
            
            **IMPORTANTE**: 
            - Arquivos são **substituídos** a cada geração (não concatenados como histórico)
            - Cada geração cria um forecast novo baseado nas configurações atuais
            - Arquivos antigos são sobrescritos
            """)
        
        st.markdown("---")
        
        # Seção 3: Página 2 - Best Estimate Simulador
        st.markdown("## 🔮 PÁGINA 2 - BEST ESTIMATE SIMULADOR {#pagina-simulador}")
        
        st.markdown("""
        ### Funcionalidades Principais
        
        **Objetivo:**
        A página 2 (Best Estimate - Simulador) permite **simular e ajustar** parâmetros do forecast
        em tempo real, visualizando o impacto das mudanças antes de salvar.
        
        **Funcionalidades:**
        
        **1. Configuração Interativa de Parâmetros:**
        - Seleção de períodos históricos para média (multiselect)
        - Exclusão de meses específicos (multiselect)
        - Configuração de sensibilidades (fixo, variável, Type 06)
        - Configuração de inflação (global e por Type 06)
        - Seleção de períodos futuros para forecast
        
        **2. Visualização em Tempo Real:**
        - Gráficos atualizados automaticamente ao alterar parâmetros
        - Tabelas interativas mostrando valores linha a linha
        - Comparação entre diferentes cenários
        
        **3. Ajustes de Volume:**
        - Permite ajustar volumes futuros manualmente
        - Visualiza impacto imediato nos custos previstos
        - Suporta diferentes volumes por período
        
        **4. Salvamento de Forecast:**
        - Botão para salvar forecast calculado
        - Salva em `dados/Forecast/forecast_completo.parquet`
        - Substitui forecast anterior
        
        **5. Análise de Sensibilidade:**
        - Permite testar diferentes valores de sensibilidade
        - Visualiza impacto de mudanças nos parâmetros
        - Útil para cenários "what-if"
        
        **6. Custos Específicos (BE Manual):**
        - Permite adicionar custos específicos com valores manuais
        - Suporta dois tipos de aplicação:
          - **Pontual**: Aplicado em meses específicos selecionados
          - **Constante**: Aplicado a partir de um mês inicial em diante
        - Rateio automático por veículo baseado em percentuais do arquivo de rateio
        - Integração automática com Account (Type 07) para buscar Type 06, Type 05, Custo e USI
        - Visualização e exclusão de custos específicos cadastrados
        - Formatação numérica com separador de milhares (formato brasileiro)
        - Tabela interativa com seleção múltipla para exclusão em lote
        - Os custos específicos são marcados como "BE Manual" na coluna Tipo
        - Integrados automaticamente ao forecast final como linhas separadas
        
        **7. Nomenclatura Atualizada:**
        - Coluna "Tipo" agora usa "BE" para forecast normal
        - Coluna "Tipo" usa "BE Manual" para custos específicos/manuais
        - Título atualizado: "Best Estimate - Previsão de Custo Total"
        - Compatibilidade automática com arquivos antigos (conversão de "Forecast" para "BE")
        """)
        
        st.markdown("---")
        
        # Seção 3.1: Custos Específicos - Detalhamento
        st.markdown("### 💰 Custos Específicos (BE Manual) - Detalhamento")
        
        st.markdown("""
        **Funcionalidade:** Permite adicionar custos específicos com valores manuais que são integrados ao forecast.
        
        **Como Funciona:**
        
        **1. Adicionar Custo Específico:**
        - Acesse a aba "➕ Adicionar Custo" na página 2
        - Preencha os campos obrigatórios:
          - **Account (Type 07)**: Seleciona o Account e busca automaticamente Type 06, Type 05, Custo e USI
          - **Oficina**: Seleciona a oficina (sem opção "Todos")
          - **Veículo**: Seleciona veículo específico ou "Todos" para rateio automático
          - **Período**: Seleciona o período (mês e ano)
          - **Tipo de Aplicação**: 
            - **Pontual**: Aplicado apenas nos meses selecionados
            - **Constante**: Aplicado a partir do mês inicial em diante
          - **Valor Total**: Valor total do custo
          - **Descrição**: Descrição opcional do custo
        
        **2. Rateio Automático:**
        - Se "Todos" for selecionado para Veículo, o sistema busca automaticamente os percentuais de rateio do arquivo `Reporting fluxo anexo.xlsx` (aba "Rateio")
        - O rateio é aplicado mês a mês conforme os percentuais do arquivo
        - Se um veículo específico for selecionado, o rateio é 100% para aquele veículo
        - O valor total é distribuído proporcionalmente entre os veículos
        
        **3. Visualizar Custos:**
        - Acesse a aba "📋 Visualizar Custos"
        - Tabela interativa com todas as colunas do formato `df_final_historico_forecast.xlsx`
        - Formatação numérica com 2 casas decimais e separador de milhares (formato brasileiro)
        - Seleção múltipla com checkboxes para exclusão em lote
        - Botão "🗑️ Deletar Selecionadas" para remover custos
        
        **4. Integração com Forecast:**
        - Os custos específicos são automaticamente incluídos no forecast final
        - Aparecem como linhas separadas com Tipo = "BE Manual"
        - Não são somados ao forecast calculado, mas adicionados como linhas independentes
        - Mantém o mesmo formato e estrutura do forecast normal
        
        **5. Persistência:**
        - Os custos específicos são salvos em `dados/Forecast/custos_especificos.parquet`
        - São carregados automaticamente ao gerar o forecast
        - Permanecem salvos até serem explicitamente excluídos
        
        **6. Formato de Dados:**
        - Os custos específicos seguem exatamente o formato de `df_final_historico_forecast.xlsx`
        - Colunas na ordem: Account, Ano, Centrocst, Custo, Fornec., Fornecedor, Mes, Oficina, Período, Soma_Percentuais, Tipo, Total, Type 05, Type 06, USI, Valor, Veículo
        - Tipo sempre preenchido como "BE Manual" para identificação
        """)
        
        st.markdown("---")
        
        # Seção 4: Página - Best Estimate (Análise)
        st.markdown("## 📊 PÁGINA - BEST ESTIMATE (ANÁLISE) {#pagina-analise}")
        
        st.markdown("""
        ### Funcionalidades Principais
        
        **Objetivo:**
        A página **Best Estimate (Análise)** no menu **TC Ext** substitui a análise legacy e entrega:
        - as **mesmas tabelas/visuais** da Home (TC Ext),
        - porém alimentadas pelos **arquivos de Forecast** gerados pelo simulador.
        
        **Funcionalidades:**
        
        **1. Fonte de dados (Forecast):**
        - Lê `dados/Forecast/forecast_completo.parquet` (custos) e `dados/Forecast/df_vol_historico.parquet` (volume)
        - Permite analisar previsões (BE) e histórico no mesmo layout
        - Expander de diagnóstico mostra paths, mtimes e contagens
        
        **2. Visualizações (mesma base da Home):**
        - Gráficos e tabelas por período, oficina, veículo
        - Mesmo padrão de filtros e formatação
        - Sem “corte” de meses futuros quando houver Forecast
        
        **3. Tabelas detalhadas (com TOTAL coerente):**
        - No modo CPU, totais são sempre `CPU = sum(Total) / sum(Volume)` (ponderado)
        - Expander opcional “Volume por período” ajuda a explicar variações do TOTAL mês a mês
        
        **4. Comparações:**
        - Permite comparar BE vs histórico dentro do mesmo layout de análise
        - Facilita validar premissas (sensibilidade/inflação) pela variação temporal
        
        **5. Integração com o simulador:**
        - O simulador gera/salva os arquivos em `dados/Forecast/`
        - A análise lê esses arquivos e atualiza as visualizações
        
        **6. Modos de visualização:**
        - **Custo Total:** Valores absolutos em R$
        - **CPU (Custo por Unidade):** Valores por unidade produzida
        - Permite alternar entre modos para diferentes análises
        """)
        
        st.markdown("---")
        
        # Seção 5: Fluxo de Dados e Processamento
        st.markdown("## 🔄 FLUXO DE DADOS E PROCESSAMENTO {#fluxo-dados-forecast}")
        
        st.markdown("""
        ### Fluxo Completo de Dados
        
        **Diagrama de Fluxo:**
        ```
        Dados Históricos (historico_consolidado/)
                │
                ├──> Carregamento (load_data)
                │       │
                │       ├──> Aplicar Filtros (Oficina, Veículo, etc.)
                │       │
                │       └──> Filtrar Períodos Selecionados
                │
                ├──> Cálculo de Médias (calcular_medias_forecast)
                │       │
                │       ├──> Agrupar por Chaves Únicas
                │       │
                │       ├──> Calcular Média de Custos
                │       │
                │       └──> Calcular Volume Médio Histórico
                │
                ├──> Cálculo de Forecast (calcular_forecast_completo)
                │       │
                │       ├──> Para cada linha:
                │       │       ├──> Obter Média Histórica
                │       │       ├──> Obter Volume Futuro
                │       │       ├──> Calcular Proporção
                │       │       ├──> Aplicar Sensibilidade
                │       │       └──> Aplicar Inflação
                │       │
                │       └──> DataFrame Completo com Forecast
                │
                └──> Salvamento (dados/Forecast/)
                        │
                        ├──> forecast_completo.parquet
                        ├──> forecast_historico.parquet
                        └──> forecast_previsao.parquet
        ```
        
        **Características do Fluxo:**
        - **Tempo Real:** Forecast é calculado em tempo real com configurações atuais
        - **Não Persistente:** Configurações (sensibilidade, inflação) não são salvas, apenas o resultado
        - **Substituição:** Cada geração substitui o forecast anterior
        - **Independência:** Cada página pode gerar seu próprio forecast
        """)
        
        st.markdown("---")
        
        # Seção 6: Arquivos Gerados
        st.markdown("## 📄 ARQUIVOS GERADOS {#arquivos-gerados-forecast}")
        
        st.markdown("""
        ### Arquivos na Pasta `dados/Forecast/`
        
        **1. forecast_completo.parquet**
        - **Conteúdo**: Forecast completo com todas as linhas calculadas
        - **Estrutura**: Mesmas colunas dos dados históricos + colunas de forecast
        - **Uso**: Fonte principal para análises e visualizações
        - **Atualização**: Substituído a cada geração de forecast
        
        **2. forecast_historico.parquet**
        - **Conteúdo**: Histórico de forecasts gerados anteriormente
        - **Estrutura**: Similar ao forecast_completo, mas com múltiplos forecasts
        - **Uso**: Análise de evolução de forecasts ao longo do tempo
        - **Atualização**: Pode ser concatenado ou substituído (depende da implementação)
        
        **3. forecast_previsao.parquet**
        - **Conteúdo**: Apenas previsões futuras (sem dados históricos)
        - **Estrutura**: Apenas períodos futuros do forecast
        - **Uso**: Análise focada apenas em previsões
        - **Atualização**: Substituído a cada geração
        
        **4. df_final_historico_forecast.parquet**
        - **Conteúdo**: Dados históricos filtrados usados para calcular o forecast
        - **Estrutura**: Dados históricos após aplicação de filtros e seleção de períodos
        - **Uso**: Referência dos dados que foram usados para calcular a média
        - **Atualização**: Gerado junto com o forecast
        
        **5. df_vol_historico.parquet**
        - **Conteúdo**: Volumes históricos usados para cálculo de proporções
        - **Estrutura**: Volumes por período, oficina, veículo
        - **Uso**: Cálculo de volume médio histórico e proporções
        - **Atualização**: Pode ser copiado do histórico consolidado ou gerado
        
        **6. custos_especificos.parquet**
        - **Conteúdo**: Custos específicos cadastrados manualmente (BE Manual)
        - **Estrutura**: Mesmo formato de `df_final_historico_forecast.xlsx` com coluna Tipo = "BE Manual"
        - **Uso**: Armazena custos específicos que são integrados ao forecast final
        - **Atualização**: Criado/modificado ao adicionar ou excluir custos específicos
        - **Localização**: `dados/Forecast/custos_especificos.parquet`
        """)
        
        st.markdown("---")
        
        # Seção 6.1: Nomenclatura e Tipos
        st.markdown("### 🏷️ Nomenclatura e Tipos de Dados")
        
        st.markdown("""
        **Coluna "Tipo" no Forecast:**
        
        O sistema utiliza a coluna "Tipo" para identificar diferentes tipos de dados no forecast:
        
        - **"Histórico"**: Dados históricos reais (não previstos)
        - **"BE"**: Best Estimate - Forecast calculado automaticamente pelo sistema
        - **"BE Manual"**: Best Estimate Manual - Custos específicos adicionados manualmente
        
        **Compatibilidade:**
        - Arquivos antigos com "Forecast" são automaticamente convertidos para "BE" ao carregar
        - Isso garante compatibilidade com versões anteriores do sistema
        
        **Filtros e Separação:**
        - O sistema separa automaticamente histórico, BE e BE Manual ao gerar arquivos
        - `forecast_historico.parquet`: Apenas dados históricos
        - `forecast_previsao.parquet`: Apenas BE e BE Manual (previsões)
        - `df_final_historico_forecast.parquet`: Consolidado com todos os tipos
        """)
        
        st.markdown("---")
        
        # Seção 7: Cenários de Uso
        st.markdown("## 📋 CENÁRIOS DE USO {#cenarios-uso-forecast}")
        
        st.markdown("### Casos Práticos Completos")
        
        with st.expander("**Cenário 1: Gerar Forecast pela Primeira Vez**", expanded=False):
            st.markdown("""
            **Situação**: Nunca gerou forecast, precisa criar previsões para próximos meses
            
            **Passo a Passo**:
            
            1. **Acessar Página 2 (Simulador)**:
               - Selecionar períodos históricos (ex: últimos 3 meses)
               - Configurar sensibilidades (Fixo: 0%, Variável: 100%)
               - Configurar inflação (ex: 5%)
               - Selecionar períodos futuros (ex: próximos 6 meses)
            
            2. **Informar Volumes Futuros**:
               - Inserir volumes esperados para cada período futuro
               - Ou usar volumes projetados automaticamente
            
            3. **Visualizar Resultados**:
               - Verificar gráficos e tabelas
               - Ajustar parâmetros se necessário
            
            4. **Salvar Forecast**:
               - Clicar em "Salvar Forecast"
               - Sistema cria `dados/Forecast/` automaticamente
               - Salva `forecast_completo.parquet`
            
            5. **Analisar na Página 3**:
               - Acessar Página 3 (Análise)
               - Carregar forecast gerado
               - Visualizar análises detalhadas
            
            **Resultado**: 
            - Pasta `dados/Forecast/` criada com forecast completo
            - Forecast disponível para análises e comparações
            """)
        
        with st.expander("**Cenário 2: Atualizar Forecast com Novos Dados**", expanded=False):
            st.markdown("""
            **Situação**: Já existe forecast, mas novos dados históricos foram adicionados
            
            **Passo a Passo**:
            
            1. **Atualizar Dados Históricos** (se necessário):
               - Executar extração de dados (Página 5) para incluir novos períodos
               - Histórico consolidado é atualizado automaticamente
            
            2. **Acessar Página 2 ou 3**:
               - Selecionar novos períodos históricos (incluindo os mais recentes)
               - Manter ou ajustar sensibilidades e inflação
            
            3. **Gerar Novo Forecast**:
               - Clicar em "Gerar Forecast" ou "Salvar Forecast"
               - Sistema recalcula com dados atualizados
            
            4. **Forecast Anterior é Substituído**:
               - `forecast_completo.parquet` é sobrescrito
               - Novo forecast reflete dados mais recentes
            
            **Resultado**: 
            - Forecast atualizado com dados mais recentes
            - Previsões mais acuradas baseadas em histórico expandido
            """)
        
        with st.expander("**Cenário 3: Testar Diferentes Cenários (What-If)**", expanded=False):
            st.markdown("""
            **Situação**: Quer testar impacto de diferentes volumes ou inflações
            
            **Passo a Passo**:
            
            1. **Acessar Página 2 (Simulador)**:
               - Configurar parâmetros base (sensibilidades, períodos históricos)
            
            2. **Testar Cenário 1**:
               - Ajustar volumes futuros (ex: +10%)
               - Visualizar impacto nos custos
               - **NÃO salvar** (apenas visualizar)
            
            3. **Testar Cenário 2**:
               - Ajustar volumes futuros (ex: -5%)
               - Visualizar impacto
               - Comparar com Cenário 1
            
            4. **Testar Diferentes Inflações**:
               - Alterar percentual de inflação
               - Ver impacto em todos os custos
               - Comparar cenários
            
            5. **Salvar Cenário Escolhido**:
               - Após decidir qual cenário usar
               - Configurar parâmetros finais
               - Salvar forecast
            
            **Resultado**: 
            - Múltiplos cenários testados sem salvar
            - Forecast final salvo com cenário escolhido
            """)
        
        with st.expander("**Cenário 4: Análise Detalhada de Forecast Gerado**", expanded=False):
            st.markdown("""
            **Situação**: Forecast já foi gerado, precisa de análises detalhadas
            
            **Passo a Passo**:
            
            1. **Acessar Página 3 (Análise)**:
               - Sistema carrega `forecast_completo.parquet` automaticamente
               - Mostra data de última atualização
            
            2. **Aplicar Filtros**:
               - Filtrar por Oficina, Veículo, Type 05, Type 06, etc.
               - Selecionar períodos específicos
            
            3. **Visualizar Gráficos**:
               - Gráficos de linha mostrando evolução
               - Gráficos de barras comparando períodos
               - Identificar tendências e padrões
            
            4. **Analisar Tabelas**:
               - Tabelas hierárquicas com drill-down
               - Detalhamento linha a linha
               - Identificar maiores custos previstos
            
            5. **Exportar para Excel**:
               - Exportar tabelas para análise externa
               - Compartilhar resultados com equipe
            
            **Resultado**: 
            - Análises detalhadas do forecast
            - Insights para tomada de decisão
            - Documentação dos resultados
            """)
        
        st.markdown("---")
        
        st.success("""
        **✅ Este capítulo descreve completamente a estrutura, atualização e funcionamento**
        das páginas de Best Estimate. Use estas informações para entender como o sistema
        organiza os dados, processa os forecasts e como cada página contribui para o processo.
        """)

# ==========================================
# SEÇÃO 6: APRESENTAÇÃO VISUAL
# ==========================================
elif indice_selecionado == "📊 Apresentação Visual":
    st.header("📊 Apresentação Visual - 5 Minutos")
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
        <h2 style="color: white; margin: 0;">📊 Apresentação Visual do Sistema TC</h2>
        <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Apresentação completa de 5 minutos com todos os slides e diagramas visuais
        </p>
    </div>
    """, unsafe_allow_html=True)

    tab_roteiro, tab_slides = st.tabs(["🎤 Roteiro (5 min)", "🧩 Slides (Markdown)"])

    with tab_roteiro:
        st.subheader("🎤 Roteiro sugerido (objetivo: clareza em 5 minutos)")
        st.markdown(
            """
            **0:00–0:30 — Contexto**
            - O que é o Portal TC e seu objetivo: decisão rápida com dados de custo/volume.
            - Dois módulos: **TC Extendido** (agregado) e **TC Veículos** (rateado por modelo).

            **0:30–1:15 — TC Ext (Home)**
            - Mostrar filtros (Ano/Período/Oficina/Veículo) e alternância **Custo Total ↔ CPU**.
            - Reforçar a regra: em CPU, o total é **ponderado por volume** (`sum(Total)/sum(Volume)`).

            **1:15–2:00 — TC Veículos (Home)**
            - Cadeia: Despesa Primária → FA → FP → D&A → FP sem Dedicada.
            - 6 tabs: TC Veículos, Análise Flex, Volume, Custos por Oficina, Tempo Produção, Dados Detalhados.
            - Seleção de veículo específico aciona rateio por tempo de produção.

            **2:00–2:45 — Waterfall**
            - Explicar "o que mudou" entre dois períodos e como o Flex Bud separa efeito volume/custo.
            - Disponível nos dois módulos (TC Ext e TC Veículos).

            **2:45–4:00 — Best Estimate**
            - **Simulador**: define premissas (sensibilidade/inflação/volume) e gera `Forecast/`.
            - **Análise BE**: layout da Home com Forecast. Cores: roxo escuro = Histórico, roxo claro = BE.
            - Disponível para TC Ext e TC Veículos.

            **4:00–5:00 — Encerramento**
            - Exportação Excel com formatação profissional.
            - Multi-moeda (BRL/USD/EUR) e fator de escala (K/M).
            - Equipe: Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus.
            """
        )
        st.info(
            "Dica: quando alguém questionar variações de TOTAL em CPU por mês, abra o expander "
            "‘Volume por período’ para mostrar que a diferença vem do denominador (volume)."
        )
    
    with tab_slides:
        # Carregar e exibir apresentação
        try:
            # Caminho relativo à raiz do projeto
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            caminho_apresentacao = os.path.join(base_path, "APRESENTACAO_5_MINUTOS_VISUAL.md")
            if os.path.exists(caminho_apresentacao):
                with open(caminho_apresentacao, 'r', encoding='utf-8') as f:
                    conteudo_apresentacao = f.read()

                # Limpar espaços extras no final das linhas e linhas vazias desnecessárias
                linhas = conteudo_apresentacao.split('\n')
                linhas_limpas = []
                linha_anterior_vazia = False

                for linha in linhas:
                    # Remover espaços no final da linha
                    linha_limpa = linha.rstrip()
                    # Remover linhas vazias consecutivas (máximo 1)
                    if not linha_limpa:
                        if not linha_anterior_vazia:
                            linhas_limpas.append('')
                        linha_anterior_vazia = True
                    else:
                        linhas_limpas.append(linha_limpa)
                        linha_anterior_vazia = False

                # Remover linhas vazias no início e fim
                while linhas_limpas and not linhas_limpas[0]:
                    linhas_limpas.pop(0)
                while linhas_limpas and not linhas_limpas[-1]:
                    linhas_limpas.pop()

                conteudo_limpo = '\n'.join(linhas_limpas)

                # Exibir apresentação usando st.markdown
                st.markdown(conteudo_limpo, unsafe_allow_html=True)
            else:
                st.warning("⚠️ Arquivo de apresentação não encontrado. Verifique se o arquivo APRESENTACAO_5_MINUTOS_VISUAL.md existe na raiz do projeto.")
        except Exception as e:
            st.error(f"❌ Erro ao carregar apresentação: {str(e)}")

# ==========================================
# SEÇÃO 7: CHATBOT DE DOCUMENTAÇÃO
# ==========================================
elif indice_selecionado == "💬 Chatbot de Documentação":
    st.header("💬 Chatbot de Documentação")
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
        <h2 style="color: white; margin: 0;">💬 Assistente Virtual de Documentação</h2>
        <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Faça perguntas sobre o sistema e receba respostas baseadas na documentação completa
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Importar chatbot
    try:
        # Adicionar diretório raiz ao path para importar chatbot
        import sys
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if base_path not in sys.path:
            sys.path.insert(0, base_path)
        from chatbot_documentacao import responder_pergunta
        
        # Inicializar histórico de conversa
        if 'historico_chat' not in st.session_state:
            st.session_state.historico_chat = []
        
        # Exibir histórico
        st.subheader("💬 Conversa")
        
        if st.session_state.historico_chat:
            for i, (pergunta, resposta, score) in enumerate(st.session_state.historico_chat):
                with st.expander(f"❓ {pergunta[:50]}...", expanded=False):
                    st.markdown(f"**Pergunta:** {pergunta}")
                    st.markdown(f"**Resposta:**")
                    st.markdown(resposta)
                    if score > 0:
                        st.caption(f"Relevância: {score:.0%}")
        else:
            st.info("💡 Faça sua primeira pergunta abaixo para começar!")
        
        st.markdown("---")
        
        # Campo de entrada
        st.subheader("📝 Faça uma Pergunta")
        
        pergunta = st.text_input(
            "Digite sua pergunta sobre o sistema:",
            placeholder="Ex: Como funciona o Best Estimate? O que é Flex Bud? Como processar dados?",
            key="input_pergunta"
        )
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            botao_perguntar = st.button("🔍 Buscar Resposta", type="primary", use_container_width=True)
        
        with col2:
            botao_limpar = st.button("🗑️ Limpar Histórico", use_container_width=True)
        
        if botao_limpar:
            st.session_state.historico_chat = []
            st.rerun()
        
        if botao_perguntar and pergunta:
            with st.spinner("🔍 Buscando na documentação..."):
                resultado = responder_pergunta(pergunta)
                
                if resultado['resposta']:
                    # Adicionar ao histórico
                    st.session_state.historico_chat.append((
                        pergunta,
                        resultado['resposta'],
                        resultado['score']
                    ))
                    
                    # Exibir resposta
                    st.success("✅ Resposta encontrada!")
                    st.markdown("**Resposta:**")
                    st.markdown(resultado['resposta'])
                    
                    if resultado['score'] > 0:
                        st.caption(f"📊 Relevância da resposta: {resultado['score']:.0%}")
                    
                    # Exibir segmentos adicionais se houver
                    if resultado['segmentos_encontrados']:
                        st.markdown("---")
                        st.subheader("📚 Informações Adicionais")
                        for i, segmento in enumerate(resultado['segmentos_encontrados'], 1):
                            with st.expander(f"Informação adicional {i}", expanded=False):
                                st.markdown(segmento)
                    
                    st.rerun()
        
        # Sugestões de perguntas
        st.markdown("---")
        st.subheader("💡 Perguntas Sugeridas")
        
        perguntas_sugeridas = [
            "O que é o Sistema TC?",
            "Como funciona o Best Estimate?",
            "O que é Flex Bud?",
            "Como funciona o rateio por veículo?",
            "Qual a diferença entre TC Ext e TC Veículos?",
            "Como funciona a sensibilidade no simulador?",
            "O que é CPU (Custo por Unidade)?",
            "Como funciona o Waterfall?",
        ]
        
        cols = st.columns(2)
        for i, pergunta_sugerida in enumerate(perguntas_sugeridas):
            with cols[i % 2]:
                if st.button(f"❓ {pergunta_sugerida}", key=f"sug_{i}", use_container_width=True):
                    # Processar pergunta sugerida diretamente
                    with st.spinner("🔍 Buscando na documentação..."):
                        resultado = responder_pergunta(pergunta_sugerida)
                        
                        if resultado['resposta']:
                            # Adicionar ao histórico
                            st.session_state.historico_chat.append((
                                pergunta_sugerida,
                                resultado['resposta'],
                                resultado['score']
                            ))
                            st.rerun()
        
    except ImportError as e:
        st.error(f"❌ Erro ao importar módulo de chatbot: {str(e)}")
        st.info("💡 Certifique-se de que o arquivo chatbot_documentacao.py existe na raiz do projeto.")
    except Exception as e:
        st.error(f"❌ Erro no chatbot: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

# Rodapé
st.markdown("---")
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    📚 Documentação Completa do Sistema TC | Versão {versao_atual} | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus</small>
</div>
""", unsafe_allow_html=True)
