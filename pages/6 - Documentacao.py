import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import json
import os
import base64
import sys
from datetime import datetime
from tc_core.presentation_docs import render_presentation_section
from versionamento import obter_versao_atual

# DiretÃ³rio raiz do projeto
if hasattr(sys, '_MEIPASS'):
    _ROOT = sys._MEIPASS
else:
    _ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ConfiguraÃ§Ã£o da pÃ¡gina
st.set_page_config(
    page_title="DocumentaÃ§Ã£o - Stellantis Cost Intelligence (SCI)",
    page_icon="ðŸ“š",
    layout="wide",
    initial_sidebar_state="expanded"
)

# FunÃ§Ã£o para obter mÃªs atual em portuguÃªs
def obter_mes_atual():
    """Retorna o mÃªs atual em portuguÃªs"""
    meses = {
        1: "Janeiro", 2: "Fevereiro", 3: "MarÃ§o", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    agora = datetime.now()
    return meses[agora.month]

# FunÃ§Ã£o para obter data e hora de atualizaÃ§Ã£o dos dados
def obter_data_atualizacao_dados():
    """Retorna a data e hora da Ãºltima atualizaÃ§Ã£o dos arquivos de dados"""
    try:
        arquivos_dados = [
            os.path.join(_ROOT, "dados", "TC_Ext", "historico_consolidado", "df_final_historico.parquet"),
            os.path.join(_ROOT, "dados", "TC_Ext", "historico_consolidado", "df_vol_historico.parquet"),
            os.path.join(_ROOT, "dados", "TC_Ext", "historico_consolidado", "BUD", "df_final_historico_BUD.parquet"),
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
                    1: "Janeiro", 2: "Fevereiro", 3: "MarÃ§o", 4: "Abril",
                    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
                    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                }
                return f"{dt.day:02d} de {meses[dt.month]} de {dt.year} Ã s {dt.hour:02d}:{dt.minute:02d}"
            except (ValueError, OSError):
                return "NÃ£o disponÃ­vel"
        return None
    except Exception:
        return None

# CabeÃ§alho compacto com data de atualizaÃ§Ã£o
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
data_atualizacao = obter_data_atualizacao_dados()

# Montar textos do cabeÃ§alho
texto_esquerda = f"ðŸ“š Stellantis Cost Intelligence (SCI) | VersÃ£o {versao_atual} | {mes_atual} {ano_atual} | Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus"
texto_direita = f"ðŸ“… Dados atualizados em: {data_atualizacao}" if data_atualizacao else ""

st.markdown(f"""
<div style='display: flex; justify-content: space-between; align-items: center; color: #fff; padding: 8px 10px; font-size: 0.85rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-bottom: 1px solid #5a4fcf; margin-bottom: 10px;'>
    <div style='flex: 1;'>{texto_esquerda}</div>
    <div style='flex: 0 0 auto; margin-left: 20px;'>{texto_direita}</div>
</div>
""", unsafe_allow_html=True)


# CSS para melhorar visualizaÃ§Ã£o
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

st.title("ðŸ“š DocumentaÃ§Ã£o â€” Stellantis Cost Intelligence (SCI)")


# Tradutor (PT/EN/FR/ES) â€” traduz a pÃ¡gina inteira via widget client-side
with st.container():
        cols = st.columns([1, 6])
        with cols[0]:
                st.markdown("**ðŸŒ Tradutor**")
        with cols[1]:
                st.markdown(
                        "<div id='sci-translate-container' style='min-height: 28px;'></div>",
                        unsafe_allow_html=True,
                )

        components.html(
                """
                <script>
                (function() {
                    try {
                        const parentWindow = window.parent;
                        const doc = parentWindow.document;

                        // Container visÃ­vel (criado via st.markdown)
                        const container = doc.getElementById('sci-translate-container');
                        if (!container) return;

                        // Elemento do tradutor
                        let el = doc.getElementById('google_translate_element');
                        if (!el) {
                            el = doc.createElement('div');
                            el.id = 'google_translate_element';
                            container.appendChild(el);
                        }

                        // Callback esperado pelo script do Google Translate
                        if (!parentWindow.googleTranslateElementInit) {
                            parentWindow.googleTranslateElementInit = function() {
                                if (!parentWindow.google || !parentWindow.google.translate) return;
                                new parentWindow.google.translate.TranslateElement(
                                    {
                                        pageLanguage: 'pt',
                                        includedLanguages: 'pt,en,fr,es',
                                        autoDisplay: false
                                    },
                                    'google_translate_element'
                                );
                            };
                        }

                        // Carrega o script uma Ãºnica vez
                        if (!doc.getElementById('google-translate-script')) {
                            const s = doc.createElement('script');
                            s.id = 'google-translate-script';
                            s.src = 'https://translate.google.com/translate_a/element.js?cb=googleTranslateElementInit';
                            doc.body.appendChild(s);
                        }
                    } catch (e) {
                        // Se a polÃ­tica do navegador/Streamlit bloquear acesso ao parent,
                        // simplesmente nÃ£o mostra o tradutor.
                    }
                })();
                </script>
                """,
                height=0,
        )

        st.caption(
                "TraduÃ§Ã£o automÃ¡tica (PT/EN/FR/ES) via Google Translate. "
                "Se o acesso externo estiver bloqueado na rede corporativa, o tradutor pode nÃ£o aparecer."
        )


# FunÃ§Ã£o para detectar caminho base correto
def get_base_path():
    """Retorna o caminho base correto para LEITURA de dados"""
    import sys
    if hasattr(sys, '_MEIPASS'):
        # Rodando no executÃ¡vel PyInstaller - apontar para _internal
        return sys._MEIPASS
    else:
        # Rodando em desenvolvimento
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _formatar_mtime(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return "NÃ£o disponÃ­vel"


@st.cache_data(show_spinner=False)
def _ler_arquivo_texto_cacheado(caminho: str, mtime: float) -> str:
    with open(caminho, "r", encoding="utf-8") as f:
        return f.read()


def _carregar_markdown(caminho: str) -> tuple[str | None, str | None, float | None]:
    if not os.path.exists(caminho):
        return None, f"Arquivo nÃ£o encontrado: {caminho}", None
    try:
        mtime = os.path.getmtime(caminho)
        return _ler_arquivo_texto_cacheado(caminho, mtime), None, mtime
    except Exception as e:
        return None, f"Erro ao carregar arquivo: {caminho} ({e})", None


def _extrair_secao_por_heading(md: str, headings: list[str]) -> str:
    """Extrai o conteÃºdo de uma seÃ§Ã£o markdown (sem o heading).

    Procura o primeiro heading encontrado em `headings` e retorna atÃ© o prÃ³ximo
    heading de nÃ­vel 2 ("## ").
    """
    if not md:
        return ""
    start = -1
    heading_encontrado = None
    for h in headings:
        start = md.find(h)
        if start != -1:
            heading_encontrado = h
            break
    if start == -1 or heading_encontrado is None:
        return ""

    start_line_end = md.find("\n", start)
    if start_line_end == -1:
        return ""
    start_content = start_line_end + 1
    end = md.find("\n## ", start_content)
    if end == -1:
        end = len(md)
    return md[start_content:end].strip()

# FunÃ§Ãµes para persistir dados da equipe
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
                'lÃ³gica e os cÃ¡lculos do sistema'
            ),
        },
        'lauro': {
            'cargo': '', 'empresa': '', 'experiencia': '',
            'linkedin': '', 'foto': None,
            'papel_projeto': 'Full-Stack Developer',
            'descricao_papel': (
                'Desenvolvendo tanto a interface quanto a '
                'lÃ³gica e os cÃ¡lculos do sistema'
            ),
        },
        'frederico': {
            'cargo': 'Manufacturing Finance Controller',
            'empresa': 'Stellantis',
            'experiencia': '',
            'linkedin': '', 'foto': None,
            'papel_projeto': 'Tech Advisor',
            'descricao_papel': (
                'OrientaÃ§Ã£o tÃ©cnica estratÃ©gica, validaÃ§Ãµes '
                'e suporte de alto nÃ­vel ao projeto'
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

# Sidebar com Ã­ndices
st.sidebar.markdown("## ðŸ“‘ Ãndice")
st.sidebar.markdown("---")

modulo_doc = "ðŸ“Œ Ambos (TC Ext + VeÃ­culos)"

# Criar Ã­ndices no sidebar
indice_selecionado = st.sidebar.radio(
    "Selecione a seÃ§Ã£o:",
    [
        "ðŸ‘¥ Equipe do SCI",
        "ðŸ“ Regras e CÃ¡lculo",
        "ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos (Normal vs CPU)",
        "ðŸ—ï¸ Arquitetura e Estrutura",
        "ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica",
        "ðŸ“¥ Guia de ExtraÃ§Ã£o de Dados",
        "ðŸ”® Guia de Best Estimate",
        "ðŸ“Š ApresentaÃ§Ã£o Visual",
        "ðŸ’¬ Chatbot de DocumentaÃ§Ã£o",
        "ðŸ”” Sistema de Alertas",
        "ðŸ“¦ Guia de Build (EXE)",
        "ðŸš€ PrÃ³ximos Passos",
    ],
    key="indice_documentacao"
)

st.markdown("---")

# ==========================================
# SEÃ‡ÃƒO 1: EQUIPE DO PROJETO
# ==========================================
if indice_selecionado == "ðŸ‘¥ Equipe do SCI":
    st.header("ðŸ‘¥ Equipe do SCI")
    
    st.markdown("""
    Esta seÃ§Ã£o apresenta os membros da equipe responsÃ¡veis pelo desenvolvimento
    e manutenÃ§Ã£o do **Stellantis Cost Intelligence (SCI)** â€” suas funÃ§Ãµes no projeto e perfis profissionais.
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

    # â”€â”€ DefiniÃ§Ã£o dos membros â”€â”€
    membros = [
        {
            'key': 'hudson',
            'nome': 'Hudson Cardin',
            'icone': 'ðŸ”§',
            'badge_class': 'team-badge-fullstack',
        },
        {
            'key': 'lauro',
            'nome': 'Lauro Paiva Junior',
            'icone': 'ðŸ“Š',
            'badge_class': 'team-badge-fullstack',
        },
        {
            'key': 'frederico',
            'nome': 'Frederico Cesar de Jesus',
            'icone': 'ðŸ§­',
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
            # â”€â”€ CabeÃ§alho: nome + badge â”€â”€
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

            # â”€â”€ Upload da foto (oculto por padrÃ£o) â”€â”€
            with st.expander("ðŸ“¸ Upload da foto", expanded=False):
                foto_up = st.file_uploader(
                    f"ðŸ“¸ Foto de {membro['nome']}",
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
                    'ðŸ‘¤</span></div>',
                    unsafe_allow_html=True,
                )

            # â”€â”€ EdiÃ§Ã£o â”€â”€
            with st.expander(
                f"âœï¸ Editar informaÃ§Ãµes", expanded=False
            ):
                with st.form(f"form_{k}"):
                    _papel = st.text_input(
                        "ðŸŽ¯ Papel no Projeto:",
                        value=dados_m.get('papel_projeto', ''),
                        key=f"papel_{k}",
                    )
                    _desc_papel = st.text_input(
                        "ðŸ“ DescriÃ§Ã£o do Papel:",
                        value=dados_m.get('descricao_papel', ''),
                        key=f"desc_papel_{k}",
                    )
                    _cargo = st.text_input(
                        "ðŸ’¼ Cargo:",
                        value=dados_m.get('cargo', ''),
                        key=f"cargo_{k}",
                    )
                    _empresa = st.text_input(
                        "ðŸ¢ Empresa:",
                        value=dados_m.get('empresa', ''),
                        key=f"empresa_{k}",
                    )
                    _exp = st.text_area(
                        "ðŸŽ¯ ExperiÃªncia:",
                        value=dados_m.get('experiencia', ''),
                        key=f"exp_{k}",
                    )
                    _linkedin = st.text_input(
                        "ðŸ”— LinkedIn:",
                        value=dados_m.get('linkedin', ''),
                        key=f"linkedin_{k}",
                    )
                    if st.form_submit_button(
                        "ðŸ’¾ Salvar", use_container_width=True
                    ):
                        dados_equipe[k]['papel_projeto'] = _papel
                        dados_equipe[k]['descricao_papel'] = _desc_papel
                        dados_equipe[k]['cargo'] = _cargo
                        dados_equipe[k]['empresa'] = _empresa
                        dados_equipe[k]['experiencia'] = _exp
                        dados_equipe[k]['linkedin'] = _linkedin
                        if salvar_dados_equipe(dados_equipe):
                            st.success("âœ… Salvo com sucesso!")
                            st.rerun()

            # â”€â”€ Perfil Profissional â”€â”€
            with st.expander("ðŸ‘¨â€ðŸ’» Perfil Profissional", expanded=False):
                if dados_m.get('cargo') and dados_m.get('empresa'):
                    st.write(
                        f"ðŸ’¼ **{dados_m['cargo']}** "
                        f"na **{dados_m['empresa']}**"
                    )
                elif dados_m.get('cargo'):
                    st.write(f"ðŸ’¼ **{dados_m['cargo']}**")
                elif dados_m.get('empresa'):
                    st.write(f"ðŸ¢ **{dados_m['empresa']}**")
                else:
                    st.write("ðŸ’¼ *Cargo nÃ£o informado*")

                if dados_m.get('experiencia'):
                    st.write(f"ðŸŽ¯ {dados_m['experiencia']}")
                else:
                    st.write("ðŸŽ¯ *ExperiÃªncia nÃ£o informada*")

                if dados_m.get('linkedin'):
                    st.markdown(
                        f"ðŸ”— [Perfil no LinkedIn]"
                        f"({dados_m['linkedin']})"
                    )
                else:
                    st.write("ðŸ”— *LinkedIn nÃ£o informado*")

    st.markdown("---")

    st.markdown("""
    ### ðŸŽ¯ Objetivos do Projeto

    O **Stellantis Cost Intelligence (SCI)** Ã© uma plataforma de anÃ¡lise de custos industriais composta por dois mÃ³dulos
    complementares, cada um atendendo um nÃ­vel de granularidade diferente:

    **ðŸ“Š TC Estendido (TC Ext)**
    - AnÃ¡lise de custos por oficina, conta e perÃ­odo
    - VisualizaÃ§Ã£o Normal (Custo Total) e CPU (Custo por Unidade)
    - Dashboard interativo com filtros (Ano, PerÃ­odo, Oficina, USI, VeÃ­culo)
    - Flex Budget: ajuste do orÃ§amento pela proporÃ§Ã£o de volume realizado
    - Waterfall Analysis: decomposiÃ§Ã£o de variaÃ§Ãµes entre perÃ­odos
    - ExportaÃ§Ã£o Excel completa com formataÃ§Ã£o profissional

    **ðŸš— TC VeÃ­culos (TC Principal)**
    - Cadeia completa: Despesa PrimÃ¡ria â†’ Custo FA â†’ Custo FP â†’ D&A â†’ FP sem Dedicada
    - Rateio proporcional por veÃ­culo (tempo de produÃ§Ã£o)
    - 6 tabs especializadas: TC VeÃ­culos, AnÃ¡lise Flex, Volume, Custos por Oficina, Tempo de ProduÃ§Ã£o, Dados Detalhados
    - Best Estimate: simulador de premissas (sensibilidade, inflaÃ§Ã£o, volume) com geraÃ§Ã£o de Forecast
    - AnÃ¡lise de Best Estimate: layout da Home alimentado por dados de Forecast

    **ðŸ”§ Capacidades Transversais**
    - ðŸš€ Cache inteligente com TTL e otimizaÃ§Ã£o de tipos de dados
    - ðŸ“¦ Dados em formato Parquet comprimido
    - ðŸ’± ConversÃ£o multi-moeda (BRL, USD, EUR) com taxas do banco de dados
    - ðŸ“Š Fator de escala configurÃ¡vel (Nenhum / K / M)
    - ðŸŽ¨ Interface moderna com tabs, grÃ¡ficos Altair e gradientes
    - âš¡ Performance otimizada para grandes volumes (70%+ reduÃ§Ã£o de memÃ³ria)
    """)

    st.markdown("<div style='height: 1.0rem;'></div>", unsafe_allow_html=True)

    _c1, c_logo, _c3 = st.columns([1, 3, 1])
    with c_logo:
        try:
            st.image("SCI_faixa.png", width="stretch")
        except TypeError:
            st.image("SCI_faixa.png", use_container_width=True)

# ==========================================
# TC VEÃCULOS: REGRAS E CÃLCULO
# ==========================================
elif indice_selecionado == "ðŸ“ Regras e CÃ¡lculo" and modulo_doc.startswith("ðŸ“Œ Ambos"):
    st.header("ðŸ“ Regras e CÃ¡lculo â€” TC Ext + TC VeÃ­culos")

    st.subheader("ðŸ“Š TC Estendido")
    _caminho_ext = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    _md_ext, _err_ext, _mtime_ext = _carregar_markdown(_caminho_ext)
    if _err_ext:
        st.error(_err_ext)
    else:
        st.caption(
            f"Fonte: {_caminho_ext} | Atualizado em: {_formatar_mtime(_mtime_ext)}"
        )
        with st.expander("ðŸ“ Regras e CÃ¡lculo â€” TC Estendido", expanded=True):
            st.markdown(
                _extrair_secao_por_heading(_md_ext, ["## 2) Regras e CÃ¡lculo â€” TC Estendido"])
            )

    _caminho_flex = os.path.join(get_base_path(), "DOCUMENTACAO_FLEX_BUD_ANO_COMPLETO.md")
    _md_flex, _err_flex, _mtime_flex = _carregar_markdown(_caminho_flex)
    if not _err_flex:
        st.caption(
            f"Fonte: {_caminho_flex} | Atualizado em: {_formatar_mtime(_mtime_flex)}"
        )
        with st.expander("ðŸ“Œ Flex Bud â€” GovernanÃ§a (Ano Completo)", expanded=False):
            st.markdown(
                _extrair_secao_por_heading(
                    _md_flex,
                    ["## 7) Flex Bud â€” Ano Completo e GovernanÃ§a"],
                )
            )

    st.markdown("---")

    st.subheader("ðŸš— TC VeÃ­culos")
    _caminho_veic = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
    _md_veic, _err_veic, _mtime_veic = _carregar_markdown(_caminho_veic)
    if _err_veic:
        st.error(_err_veic)
        st.stop()

    st.caption(
        f"Fonte: {_caminho_veic} | Atualizado em: {_formatar_mtime(_mtime_veic)}"
    )
    with st.expander("ðŸ’° Cadeia de Custos", expanded=True):
        st.markdown(_extrair_secao_por_heading(_md_veic, ["## 2) Cadeia de Custos TC VeÃ­culos"]))
    with st.expander("ðŸš— Rateio por VeÃ­culo", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md_veic, ["## 3) Processo de Rateio por VeÃ­culo"]))
    with st.expander("ðŸ“Š Flex Budget", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md_veic, ["## 4) Flex Budget (TC VeÃ­culos)"]))
    with st.expander("ðŸ“ˆ CPU (Custo por Unidade)", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md_veic, ["## 5) CPU (Custo por Unidade)"]))
    with st.expander("ðŸŽ¯ KPIs (Topo e Resumo)", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md_veic, ["## 6) KPIs do TC VeÃ­culos"]))
    with st.expander("ðŸŽ›ï¸ Filtros", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md_veic, ["## 7) Filtros do TC VeÃ­culos"]))
    with st.expander("ðŸ”® Best Estimate â€” Premissas", expanded=False):
        st.markdown(
            _extrair_secao_por_heading(
                _md_veic,
                ["## 9) Premissas do Simulador Best Estimate"],
            )
        )

    st.stop()

elif indice_selecionado == "ðŸ“ Regras e CÃ¡lculo" and modulo_doc == "ðŸš— TC VeÃ­culos":
    st.header("ðŸ“ Regras e CÃ¡lculo â€” TC VeÃ­culos")

    _caminho = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
    _md, _err, _mtime = _carregar_markdown(_caminho)
    if _err:
        st.error(_err)
        st.stop()

    st.caption(f"Fonte: {_caminho} | Atualizado em: {_formatar_mtime(_mtime)}")

    with st.expander("ðŸ’° Cadeia de Custos", expanded=True):
        st.markdown(_extrair_secao_por_heading(_md, ["## 2) Cadeia de Custos TC VeÃ­culos"]))
    with st.expander("ðŸš— Rateio por VeÃ­culo", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md, ["## 3) Processo de Rateio por VeÃ­culo"]))
    with st.expander("ðŸ“Š Flex Budget", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md, ["## 4) Flex Budget (TC VeÃ­culos)"]))
    with st.expander("ðŸ“ˆ CPU (Custo por Unidade)", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md, ["## 5) CPU (Custo por Unidade)"]))
    with st.expander("ðŸŽ¯ KPIs (Topo e Resumo)", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md, ["## 6) KPIs do TC VeÃ­culos"]))
    with st.expander("ðŸŽ›ï¸ Filtros", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md, ["## 7) Filtros do TC VeÃ­culos"]))
    with st.expander("ðŸ”® Best Estimate â€” Premissas", expanded=False):
        st.markdown(_extrair_secao_por_heading(_md, ["## 9) Premissas do Simulador Best Estimate"]))

    st.stop()

    st.info(
        "ðŸ“Œ **MÃ³dulo TC VeÃ­culos** â€” Regras de cÃ¡lculo especÃ­ficas para "
        "anÃ¡lise de custo de produÃ§Ã£o de veÃ­culos."
    )

    with st.expander("ðŸ’° **ComposiÃ§Ã£o de Custos**", expanded=True):
        st.markdown("""
        ### ðŸ”— Cadeia de Custos TC VeÃ­culos

        ```
        Despesa PrimÃ¡ria
                    Ã— Rateio FA
                    = Custo FA (Fluxo Anexo)

                Custo FP (Fluxo Principal)
                    = Despesa PrimÃ¡ria âˆ’ Custo FA

        D&A Dedicado = parcela de D&A atribuÃ­da diretamente ao veÃ­culo
        FP sem Dedicada = Custo FP âˆ’ D&A Dedicado
        ```

        **Colunas MonetÃ¡rias** (recebem conversÃ£o de moeda e fator):
        - `Despesa Primaria`, `Custo FA`, `Custo FP`, `D&A dedicado`, `FP sem Dedicada`

        **Redis** â€” NÃ£o Ã© uma coluna nem um `Account` fixo.
        Redis entra como **linhas adicionais** vindas da aba **massa - REDIS**, marcadas com `_fonte_redis=True`.

        **KPI Redis:**
        > Redis = Î£ `Despesa Primaria` nas linhas com `_fonte_redis=True` (valores tipicamente negativos por serem receita)
        """)

    with st.expander("ðŸš— **Rateio por VeÃ­culo**", expanded=False):
        st.markdown("""
        ### ðŸ“Š Processo de Rateio

        O custo da oficina Ã© **rateado** aos veÃ­culos proporcionalmente ao **tempo de produÃ§Ã£o**:

        - **Percentual(v,o)** = TempoVeic(v,o) / Î£ TempoVeic(v,o)
        - **CustoRateado(v,o)** = FPsemDedicada(o) Ã— Percentual(v,o)
        - **CustoFPVeiculo(v,o)** = CustoRateado(v,o) + D&A Dedicado(v,o)

        **Dados Consolidados vs Rateados:**

        | SeleÃ§Ã£o | Fonte BUD | Fonte Real |
        |---------|-----------|------------|
        | Todos | `df_principal_BUD.parquet` | `df_principal.parquet` |
        | VeÃ­culo especÃ­fico | `df_veiculos_custo_fp_BUD.parquet` | `df_veiculos_custo_fp.parquet` |

        > Quando **VeÃ­culo = "Todos"**: dados consolidados.
        > Quando **VeÃ­culo = modelo especÃ­fico**: dados rateados com `Custo FP Veiculo`.
        """)

    with st.expander("ðŸ“Š **Flex Budget**", expanded=False):
        st.markdown("""
        ### ðŸ”„ Conceito

        O Budget Flex ajusta o orÃ§amento pela proporÃ§Ã£o de volume realizado:
        - Custos **fixos** permanecem iguais ao Budget
        - Custos **variÃ¡veis** sÃ£o ajustados pela proporÃ§Ã£o de volume

        ### ðŸ“ FÃ³rmulas

        - **ProporÃ§Ã£o** = Volume Realizado / Volume Budget
        - **Flex fixo** = BUD fixo (sem alteraÃ§Ã£o)
        - **Flex variÃ¡vel** = BUD variÃ¡vel Ã— ProporÃ§Ã£o
        - **Flex total** = Flex fixo + Flex variÃ¡vel

        ### ðŸ·ï¸ ClassificaÃ§Ã£o Fixo/VariÃ¡vel

        A coluna `Custo` determina a classificaÃ§Ã£o:
        - Valores que comeÃ§am com `"Fix"` (case-insensitive) â†’ **Fixo**
        - Todos os demais â†’ **VariÃ¡vel**

        ```python
        mask_fixo = df['Custo'].str.lower().str.startswith('fix')
        ```
        """)

    with st.expander("ðŸ“ˆ **CPU (Custo por Unidade)**", expanded=False):
        st.markdown("""
        ### ðŸ’² FÃ³rmula

        **CPU = Custo Total / Volume Total**

        Com proteÃ§Ã£o contra divisÃ£o por zero:
        ```python
        CPU = np.where(volume != 0, custo / volume, 0.0)
        ```

        **Quando o tipo de visualizaÃ§Ã£o Ã© CPU:**
        - Cada mÃ©trica Ã© dividida pelo volume total
        - O sistema recalcula CPU **apÃ³s** agregaÃ§Ãµes (nunca soma/mÃ©dia de CPU)
        - O fator K/M Ã© aplicado nas colunas monetÃ¡rias antes do cÃ¡lculo; para CPU sem escala, usar `Fator = Nenhum`
        - Volumes de BUD e Actual sÃ£o usados conforme o contexto
        """)

    with st.expander("ðŸŽ¯ **KPIs do TC VeÃ­culos**", expanded=False):
        st.markdown("""
        ### ðŸ“Š KPIs do Topo (fora das tabs)

        | KPI | FÃ³rmula |
        |-----|---------|
        | Desp. PrimÃ¡ria | Î£ Despesa Primaria |
        | Custo FA | Î£ Custo FA |
        | Redis | Î£ Despesa Primaria (linhas `_fonte_redis=True`, origem: massa - REDIS) |
        | Custo FP | Î£ Custo FP |
        | D&A Dedicada | Î£ D&A dedicado |
        | FP sem Dedicada | Î£ FP sem Dedicada |

        ### ðŸ“Š KPIs do Resumo TC VeÃ­culos

        | KPI | FÃ³rmula |
        |-----|---------|
        | BUD | BUD fixo + BUD variÃ¡vel |
        | Flex Bud âˆ’ BUD | Flex total âˆ’ BUD total |
        | Flex BUD | BUD fixo + BUD variÃ¡vel Ã— ProporÃ§Ã£o |
        | Real âˆ’ Flex Bud | Real total âˆ’ Flex total |
        | Real | Î£ Custo FP Real |
        | Real / Flex Bud | Real / Flex BUD (%) |
        """)

    with st.expander("ðŸŽ¯ **Arquitetura de Filtros**", expanded=False):
        st.markdown("""
        ### ðŸ” Filtros do TC VeÃ­culos

        | Filtro | Tipo | Comportamento |
        |--------|------|---------------|
        | Oficina | multiselect | "Todos" ou seleÃ§Ã£o mÃºltipla |
        | Tipo Custo | multiselect | Fixo/VariÃ¡vel ou todos |
        | VeÃ­culo | **selectbox** | "Todos" (consolidado) ou **1 veÃ­culo** (rateado) |
        | PerÃ­odo | multiselect | "Todos" ou seleÃ§Ã£o de meses |

        **Cascading:** A seleÃ§Ã£o de Oficina filtra os VeÃ­culos disponÃ­veis:
        ```python
        _df_filt_ofi = df[df['Oficina'].isin(oficinas_selecionadas)]
        veiculos = sorted(_df_filt_ofi['VeÃ­culo'].dropna().unique())
        ```

        **Filtros globais:** Afetam KPIs, grÃ¡ficos e AnÃ¡lise Flex simultaneamente.
        """)

    with st.expander("ðŸ“ˆ **Sensibilidade e Volume (Best Estimate)**", expanded=False):
        st.markdown("""
        ### ðŸ”® Premissas do Simulador BE

        O Simulador de Best Estimate permite configurar premissas de **sensibilidade**, **inflaÃ§Ã£o**
        e **volume** para projetar cenÃ¡rios futuros:

        **FÃ³rmula Geral:**
        ```
        BE = MÃ©dia_HistÃ³rica Ã— Fator_VariaÃ§Ã£o Ã— Fator_InflaÃ§Ã£o
        ```

        Onde:
        - `Fator_VariaÃ§Ã£o` = 1 + (VariaÃ§Ã£o_Volume Ã— Sensibilidade)
        - `Fator_InflaÃ§Ã£o` = 1 + (InflaÃ§Ã£o / 100)
        - `VariaÃ§Ã£o_Volume` = (Volume_Futuro / Volume_MÃ©dio_HistÃ³rico) âˆ’ 1

        **Sensibilidade (impacto do volume no custo):**
        - Controla o quanto a variaÃ§Ã£o de volume afeta o custo
        - Pode ser configurada por oficina (Type 06) ou global
        - Custo Fixo: sensibilidade = 0% â†’ custo nÃ£o varia com o volume
        - Custo VariÃ¡vel: sensibilidade = 100% â†’ custo varia proporcionalmente ao volume

        **Volume:**
        - Define o volume de produÃ§Ã£o projetado por veÃ­culo
        - Usado para calcular a variaÃ§Ã£o de volume, Flex Budget e CPU do Forecast
        - Quando o custo nÃ£o tem dimensÃ£o VeÃ­culo, o volume mÃ©dio Ã© usado diretamente (`.mean()`)
        - Quando hÃ¡ VeÃ­culo, o volume Ã© somado por grupo (`.sum()`)

        **InflaÃ§Ã£o:**
        - Aplica % de reajuste sobre **todos** os custos (fixos e variÃ¡veis)
        - Ã‰ aplicada **apÃ³s** o ajuste por sensibilidade
        - FÃ³rmula: `Custo_Final = Custo_Ajustado_Sensibilidade Ã— (1 + InflaÃ§Ã£o/100)`

        **Resultado por tipo de custo:**
        - **Custo Fixo BE** = MÃ©dia HistÃ³rica Ã— (1 + InflaÃ§Ã£o%) â€” sem ajuste de volume
        - **Custo VariÃ¡vel BE** = MÃ©dia HistÃ³rica Ã— (Vol_Futuro / Vol_HistÃ³rico) Ã— (1 + InflaÃ§Ã£o%)

        ### ðŸ“Š GeraÃ§Ã£o de Forecast

        O simulador gera arquivos em `dados/TC_Principal/Forecast/`:
        - `forecast_completo.parquet` â€” Dados projetados mÃªs a mÃªs
        - `premissas.json` â€” Premissas utilizadas (sensibilidade, inflaÃ§Ã£o, volume)

        Estes dados alimentam a pÃ¡gina **Best Estimate (AnÃ¡lise)**, que usa o mesmo
        layout da Home (com grÃ¡ficos e KPIs) mas com dados de Forecast.
        """)

# ==========================================
# SEÃ‡ÃƒO 2: REGRAS E CÃLCULO â€” TC ESTENDIDO
# ==========================================
elif indice_selecionado == "ðŸ“ Regras e CÃ¡lculo":
    st.header("ðŸ“ Regras e CÃ¡lculo â€” TC Estendido")

    _caminho = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    _md, _err, _mtime = _carregar_markdown(_caminho)
    if _err:
        st.error(_err)
        st.stop()

    st.caption(f"Fonte: {_caminho} | Atualizado em: {_formatar_mtime(_mtime)}")
    st.markdown(_extrair_secao_por_heading(_md, ["## 2) Regras e CÃ¡lculo â€” TC Estendido"]))
    st.markdown("---")
    st.markdown(_extrair_secao_por_heading(_md, ["## 7) Flex Bud â€” Ano Completo e GovernanÃ§a"]))
    st.stop()

    # ConteÃºdo antigo removido: esta seÃ§Ã£o agora Ã© renderizada diretamente do Markdown oficial.
    
    st.markdown("""
    Esta seÃ§Ã£o documenta todas as regras de cÃ¡lculo, filtros e metodologias utilizadas no projeto.
    **IMPORTANTE:** Esta documentaÃ§Ã£o serve como referÃªncia para garantir que todos os cÃ¡lculos sejam
    reproduzidos de forma idÃªntica, permitindo que a IA consulte e refaÃ§a qualquer cÃ¡lculo do sistema.
    
    A documentaÃ§Ã£o estÃ¡ organizada em expanders para facilitar a navegaÃ§Ã£o. Cada seÃ§Ã£o contÃ©m explicaÃ§Ãµes
    detalhadas das regras, fÃ³rmulas matemÃ¡ticas completas e exemplos prÃ¡ticos para facilitar o entendimento.
    """)
    
    st.markdown("---")
    
    # EXPANDER 1: CÃ¡lculos Principais
    with st.expander("ðŸ”¢ **CÃ¡lculos Principais e MÃ©tricas Fundamentais**", expanded=False):
        with st.expander("ðŸ“Š **CPU (Custo por Unidade)**", expanded=False):
            st.markdown("""
            ### ðŸ“Š CPU (Custo por Unidade)
            
            O **CPU (Custo por Unidade)** Ã© uma mÃ©trica fundamental que representa o custo mÃ©dio por unidade de produÃ§Ã£o.
            Ã‰ calculado dividindo o custo total pelo volume de produÃ§Ã£o.
            
            **FÃ³rmula MatemÃ¡tica:**
            ```
            CPU = Custo_Total / Volume_Total
            ```
            
            Onde:
            - `Custo_Total` = Soma de todos os custos individuais apÃ³s agrupamento
            - `Volume_Total` = Soma de todos os volumes apÃ³s agrupamento
            
            **âš ï¸ REGRA CRÃTICA:** O CPU deve ser calculado **APÃ“S** o agrupamento dos dados, nunca antes.
            Esta Ã© uma das regras mais importantes do sistema, pois calcular CPU antes de agrupar resulta em valores incorretos.
            
            **Por que calcular apÃ³s agrupamento?**
            
            A mÃ©dia aritmÃ©tica de CPUs individuais nÃ£o Ã© igual ao CPU do total agregado. Isso ocorre porque o CPU Ã© uma
            razÃ£o (divisÃ£o), e a mÃ©dia de razÃµes nÃ£o Ã© igual Ã  razÃ£o das mÃ©dias.
            
            **Exemplo Ilustrativo:**
            
            Considere duas linhas de dados:
            - **Linha 1:** Custo Total = R$ 100, Volume = 10 unidades -> CPU = R$ 10,00/unidade
            - **Linha 2:** Custo Total = R$ 200, Volume = 40 unidades -> CPU = R$ 5,00/unidade
            
            **MÃ©todo Incorreto (calcular CPU antes de agrupar):**
            - CPU mÃ©dio = (R$ 10,00 + R$ 5,00) / 2 = **R$ 7,50/unidade** [INCORRETO]
            
            **MÃ©todo Correto (calcular CPU apÃ³s agrupar):**
            - Custo Total Agregado = R$ 100 + R$ 200 = R$ 300
            - Volume Total Agregado = 10 + 40 = 50 unidades
            - CPU Agregado = R$ 300 / 50 = **R$ 6,00/unidade** [CORRETO]
            
            A diferenÃ§a entre R$ 7,50 e R$ 6,00 pode parecer pequena, mas em grandes volumes de dados essa discrepÃ¢ncia
            se acumula e resulta em anÃ¡lises completamente incorretas.
            
            **InterpretaÃ§Ã£o do CPU:**
            - **CPU baixo:** Indica eficiÃªncia operacional, menor custo por unidade produzida
            - **CPU alto:** Indica ineficiÃªncia ou custos elevados por unidade produzida
            - **VariaÃ§Ã£o de CPU:** MudanÃ§as no CPU entre perÃ­odos indicam variaÃ§Ãµes na eficiÃªncia operacional
            """)
        
        with st.expander("ðŸ’° **Custo Total**", expanded=False):
            st.markdown("""
            ### ðŸ’° Custo Total
        
        O **Custo Total** representa a soma de todos os custos individuais apÃ³s a aplicaÃ§Ã£o de filtros e agrupamentos.
        
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Custo_Total = Î£(Custo_Individual)
        ```
        
        Onde `Î£` representa a soma de todos os custos individuais que atendem aos critÃ©rios de filtragem.
        
        **Regras de CÃ¡lculo:**
        - Sempre somar valores individuais, nunca calcular mÃ©dia
        - Aplicar todos os filtros antes de realizar o agrupamento
        - Considerar apenas valores que atendem aos critÃ©rios de seleÃ§Ã£o
        - NÃ£o incluir valores nulos ou zerados no cÃ¡lculo
        
        **Agrupamento por DimensÃµes:**
        
        O custo total pode ser calculado para diferentes nÃ­veis de agregaÃ§Ã£o:
        - Por perÃ­odo (mÃªs, trimestre, semestre, ano)
        - Por oficina
        - Por veÃ­culo
        - Por categoria de custo (Type 05, Type 06, Account)
        - Por combinaÃ§Ã£o de dimensÃµes
        
        **InterpretaÃ§Ã£o:**
        - **Custo Total crescente:** Indica aumento nos gastos operacionais
        - **Custo Total decrescente:** Indica reduÃ§Ã£o nos gastos operacionais
        - **ComparaÃ§Ã£o entre perÃ­odos:** Permite identificar tendÃªncias e variaÃ§Ãµes
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ðŸ”„ Fator de ConversÃ£o (K/M)
        
        Os **Fatores de ConversÃ£o** sÃ£o utilizados para facilitar a visualizaÃ§Ã£o de valores muito grandes,
        convertendo-os para unidades mais legÃ­veis (milhares ou milhÃµes).
        
        **Fatores DisponÃ­veis:**
        - **K (milhares):** Divide o valor por 1.000
        - **M (MilhÃµes):** Divide o valor por 1.000.000
        - **Nenhum:** MantÃ©m o valor original
        
        **FÃ³rmulas MatemÃ¡ticas:**
        ```
        Valor_K = Valor_Original / 1.000
        Valor_M = Valor_Original / 1.000.000
        ```
        
        **âš ï¸ REGRA CRÃTICA:** O fator de conversÃ£o **NÃƒO** deve ser aplicado no modo **CPU (Custo por Unidade)**.
        
        **Por que nÃ£o aplicar em CPU?**
        
        O CPU jÃ¡ Ã© uma razÃ£o (divisÃ£o entre Custo Total e Volume). Se aplicarmos o fator de conversÃ£o ao Custo Total
        antes de calcular o CPU, estarÃ­amos dividindo duas vezes, o que resultaria em valores completamente incorretos.
        
        **Exemplo:**
        - Custo Total Original: R$ 1.000.000
        - Volume: 10.000 unidades
        - CPU Correto: R$ 1.000.000 / 10.000 = **R$ 100,00/unidade** [CORRETO]
        
        Se aplicÃ¡ssemos o fator K antes:
        - Custo Total com K: R$ 1.000 K
        - CPU Incorreto: R$ 1.000 K / 10.000 = **R$ 0,10/unidade** [INCORRETO] (1000 vezes menor!)
        
        **Ordem de AplicaÃ§Ã£o das TransformaÃ§Ãµes:**
        
        1. **Primeiro:** Aplicar fator de conversÃ£o (K/M) - apenas em modo Custo Total
        2. **Segundo:** Converter moeda (se necessÃ¡rio)
        3. **Terceiro:** Realizar cÃ¡lculos (CPU, Flex Bud, diferenÃ§as, etc.)
        
        Esta ordem garante que todas as transformaÃ§Ãµes sejam aplicadas corretamente e que os resultados finais
        sejam consistentes e precisos.
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ðŸ“… Agrupamento por PerÃ­odo
        
        O **Agrupamento por PerÃ­odo** permite consolidar dados em diferentes intervalos de tempo, facilitando
        anÃ¡lises comparativas e identificaÃ§Ã£o de tendÃªncias.
        
        **Estrutura de PerÃ­odos:**
        
        Quando os dados contÃªm informaÃ§Ã£o de **Ano**, o sistema cria uma coluna combinada `PerÃ­odo_Ano` que
        agrupa tanto o perÃ­odo quanto o ano:
        ```
        PerÃ­odo_Ano = PerÃ­odo + " " + Ano
        ```
        
        Exemplo: "Janeiro 2024", "Fevereiro 2024", etc.
        
        **Agrupamento com Ano:**
        - DimensÃµes de agrupamento: `['Ano', 'PerÃ­odo']`
        - Permite comparaÃ§Ãµes ano a ano
        - Facilita anÃ¡lises de tendÃªncias de longo prazo
        
        **Agrupamento sem Ano:**
        - DimensÃµes de agrupamento: `['PerÃ­odo']`
        - Ãštil quando todos os dados sÃ£o do mesmo ano
        - Simplifica anÃ¡lises mensais ou trimestrais
        
        **FÃ³rmula de AgregaÃ§Ã£o:**
        ```
        Custo_Total_Agrupado = Î£(Custo_Individual) agrupado por PerÃ­odo
        Volume_Total_Agrupado = Î£(Volume_Individual) agrupado por PerÃ­odo
        ```
        
        **InterpretaÃ§Ã£o:**
        - Permite identificar sazonalidades e padrÃµes temporais
        - Facilita comparaÃ§Ãµes entre perÃ­odos equivalentes
        - Suporta anÃ¡lises de tendÃªncias e projeÃ§Ãµes
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ðŸ“ˆ CÃ¡lculo de DiferenÃ§as e Ratios
        
        As **DiferenÃ§as e Ratios** sÃ£o mÃ©tricas essenciais para anÃ¡lise de desempenho, permitindo comparar
        valores reais com valores planejados ou ajustados.
        
        **1. DiferenÃ§a Flex Bud - BUD:**
        
        Esta mÃ©trica compara o Budget FlexÃ­vel (ajustado pelo volume real) com o Budget original planejado.
        
        **FÃ³rmula:**
        ```
        Delta_Flex_Bud = Flex_BUD - BUD
        ```
        
        **InterpretaÃ§Ã£o:**
        - **Valor positivo:** Flex Bud > BUD (custo ajustado maior que o planejado)
        - **Valor negativo:** Flex Bud < BUD (custo ajustado menor que o planejado)
        - **Zero:** Flex Bud = BUD (custo ajustado igual ao planejado)
        
        **2. DiferenÃ§a Total - Flex Bud:**
        
        Esta mÃ©trica compara o custo real com o Budget FlexÃ­vel, indicando a eficiÃªncia operacional.
        
        **FÃ³rmula:**
        ```
        Delta_Total_Flex = Total - Flex_BUD
        ```
        
        **InterpretaÃ§Ã£o:**
        - **Valor positivo:** Total > Flex Bud (ineficiÃªncia operacional)
        - **Valor negativo:** Total < Flex Bud (eficiÃªncia operacional)
        - **Zero:** Total = Flex Bud (desempenho exatamente como esperado)
        
        **3. Ratio Total / Flex Bud:**
        
        Esta mÃ©trica expressa o desempenho real como percentual do Budget FlexÃ­vel.
        
        **FÃ³rmula:**
        ```
        Ratio = Total / Flex_BUD
        Percentual = Ratio * 100%
        ```
        
        **InterpretaÃ§Ã£o:**
        - **< 100%:** Total < Flex Bud (melhor que esperado, eficiÃªncia operacional)
        - **= 100%:** Total = Flex Bud (exatamente como esperado)
        - **> 100%:** Total > Flex Bud (pior que esperado, ineficiÃªncia operacional)
        
        **Exemplo PrÃ¡tico:**
        - Flex Bud = R$ 500.000
        - Total Real = R$ 520.000
        - Ratio = 520.000 / 500.000 = 1,04 = **104%**
        - InterpretaÃ§Ã£o: O custo real estÃ¡ 4% acima do Budget FlexÃ­vel, indicando ineficiÃªncia operacional
        """)
    
    # EXPANDER 2: Flex Bud
    with st.expander("ðŸ”„ **CÃ¡lculo de Flex Bud (Budget FlexÃ­vel)**", expanded=False):
        with st.expander("ðŸ“‹ **Conceito e Regras Fundamentais**", expanded=False):
            st.markdown("""
            ### Conceito
            
            **Flex Bud** (Budget FlexÃ­vel) Ã© um valor ajustado que considera a variaÃ§Ã£o de volume,
            aplicando regras diferentes para custos fixos e **nÃ£oâ€‘fixos**.
            
            **IMPORTANTE:** Existem dois contextos diferentes de cÃ¡lculo:
            1. **Real x Real** (Waterfall): Compara dois perÃ­odos reais (MÃªs 1 vs MÃªs 2)
            2. **Real x Budget** (TC Ext): Compara perÃ­odo real vs budget planejado
            """)
            
            st.markdown("---")
            
            st.markdown("## ðŸ“‹ Regras Fundamentais: Fixo vs NÃ£oâ€‘Fixo")
            
            st.markdown("""
            ### Regra Geral para Custos Fixos
            
            **PrincÃ­pio:** Custos fixos NÃƒO variam com o volume de produÃ§Ã£o.
            
            **FÃ³rmula Geral:**
            ```
            Flex_Fixo = Valor_Original_Fixo
            ```
            
            **ExplicaÃ§Ã£o:**
            - Independente da variaÃ§Ã£o de volume, o custo fixo permanece constante
            - Exemplos: Aluguel, salÃ¡rios fixos, depreciaÃ§Ã£o
            - Sensibilidade ao volume: **0%** (zero por cento)
            """)
            
            st.markdown("---")
            
            st.markdown("""
            ### Regra Geral para Custos NÃ£oâ€‘Fixos
            
            **PrincÃ­pio:** Custos **nÃ£oâ€‘fixos** variam PROPORCIONALMENTE ao volume de produÃ§Ã£o.
            
            **FÃ³rmula Geral:**
            ```
            Flex_NÃ£oFixo = Valor_Original_NÃ£oFixo * (Volume_Novo / Volume_Original)
            ```
            
            **ExplicaÃ§Ã£o:**
            - Se o volume dobra, o custo **nÃ£oâ€‘fixo** escala proporcionalmente
            - Se o volume reduz pela metade, o custo **nÃ£oâ€‘fixo** escala proporcionalmente
            - Exemplos: componentes variÃ¡veis e demais classificaÃ§Ãµes que nÃ£o sejam Fixo
            - Sensibilidade ao volume: **100%** (cem por cento)
            """)
        
        # Ler o conteÃºdo do Flex Bud que estÃ¡ mais abaixo no arquivo
        # Por enquanto, vou adicionar um placeholder e depois mover o conteÃºdo correto
        st.info("ðŸ“š ConteÃºdo detalhado do Flex Bud serÃ¡ movido para cÃ¡...")
    
    # EXPANDER 3: Volumes
    with st.expander("ðŸ“Š **CÃ¡lculo de Volumes**", expanded=False):
        with st.expander("ðŸ“ **Fonte de Dados de Volume**", expanded=False):
            st.markdown("""
            ### ðŸ“ Fonte de Dados de Volume
            
            Os dados de volume sÃ£o armazenados em arquivos Parquet otimizados para garantir performance e eficiÃªncia
            no processamento. O sistema utiliza diferentes arquivos de volume dependendo do contexto de anÃ¡lise.
            
            **Arquivos de Volume:**
            
            - **`df_vol_historico.parquet`:** Dados histÃ³ricos consolidados de volume
            - **`df_vol.parquet`:** Dados de volume por ano especÃ­fico
            - **`df_vol_historico_BUD.parquet`:** Dados de volume do budget histÃ³rico
            
            **Estrutura dos Dados de Volume:**
            
            Os arquivos de volume contÃªm as seguintes colunas obrigatÃ³rias:
            - **`Volume`:** Quantidade de unidades produzidas
            - **`PerÃ­odo`:** PerÃ­odo de referÃªncia (mÃªs, trimestre, etc.)
            - **`Oficina`:** IdentificaÃ§Ã£o da oficina
            - **`VeÃ­culo`:** IdentificaÃ§Ã£o do veÃ­culo
            
            E a seguinte coluna opcional:
            - **`Ano`:** Ano de referÃªncia (quando disponÃ­vel)
            
            **SincronizaÃ§Ã£o com Dados de Custo:**
            
            Os dados de volume sÃ£o estruturados de forma a permitir sincronizaÃ§Ã£o perfeita com os dados de custo,
            garantindo que o cÃ¡lculo de CPU seja preciso e consistente.
            """)
        
        with st.expander("âš ï¸ **REGRA CRÃTICA: Filtragem de Volumes**", expanded=False):
            st.markdown("""
            ### âš ï¸ REGRA CRÃTICA: Filtragem de Volumes
            
            **PrincÃ­pio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta regra Ã© absolutamente crÃ­tica para garantir a precisÃ£o do cÃ¡lculo de CPU.
            
            **Processo de Filtragem:**
            
            O processo de filtragem de volumes segue os seguintes passos:
            
            1. **Criar conjunto filtrado:** Iniciar com uma cÃ³pia dos dados de volume originais
            2. **Aplicar filtros sincronizados:** Usar os valores Ãºnicos extraÃ­dos dos dados de custo filtrados
            3. **Filtrar por perÃ­odo:** Aplicar filtro especÃ­fico para o perÃ­odo de anÃ¡lise
            4. **Agrupar e somar:** Agrupar por perÃ­odo e somar os volumes
            
            **FÃ³rmula de AgregaÃ§Ã£o:**
            
            ```
            Volume_Total = Î£(Volume_Individual) agrupado por PerÃ­odo
            ```
            
            Onde `Î£` representa a soma de todos os volumes individuais que atendem aos critÃ©rios de filtragem.
            
            **ImportÃ¢ncia da ConsistÃªncia:**
            
            A consistÃªncia entre os filtros aplicados aos dados de custo e volume Ã© essencial porque:
            - O CPU Ã© calculado como Custo Total / Volume Total
            - Se os filtros forem diferentes, o CPU resultante serÃ¡ completamente incorreto
            - AnÃ¡lises baseadas em CPU incorreto podem levar a decisÃµes de negÃ³cio equivocadas
            
            **Exemplo de Impacto:**
            
            Se vocÃª filtrar os custos para uma oficina especÃ­fica mas nÃ£o filtrar os volumes da mesma forma:
            - Custo Total (filtrado): R$ 50.000 (apenas Oficina A)
            - Volume Total (nÃ£o filtrado): 100.000 unidades (todas as oficinas)
            - CPU Incorreto: R$ 50.000 / 100.000 = R$ 0,50/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 50.000 (Oficina A)
            - Volume Total (filtrado): 10.000 unidades (Oficina A)
            - CPU Correto: R$ 50.000 / 10.000 = R$ 5,00/unidade [CORRETO]
            """)
    
    # EXPANDER 4: Moeda e Taxas
    with st.expander("ðŸ’± **Moeda e Taxas de CÃ¢mbio**", expanded=False):
        with st.expander("ðŸ’± **Moedas Suportadas**", expanded=False):
            st.markdown("""
            ### ðŸ’± Moedas Suportadas
            
            O sistema suporta conversÃ£o entre diferentes moedas para facilitar anÃ¡lises internacionais e comparaÃ§Ãµes
            com dados de outras unidades de negÃ³cio. As moedas disponÃ­veis sÃ£o:
            
            - **BRL (R$):** Real Brasileiro - moeda base do sistema
            - **USD ($):** DÃ³lar Americano
            - **EUR:** Euro
            
            **Moeda Base:**
            
            O Real Brasileiro (BRL) Ã© a moeda base do sistema. Todos os valores sÃ£o originalmente armazenados em BRL,
            e as conversÃµes para outras moedas sÃ£o realizadas multiplicando os valores pela taxa de cÃ¢mbio correspondente.
            """)
        
        with st.expander("ðŸ“Š **Taxas de CÃ¢mbio**", expanded=False):
            st.markdown("""
            ### ðŸ“Š Taxas de CÃ¢mbio
            
            As **Taxas de CÃ¢mbio** definem a relaÃ§Ã£o de conversÃ£o entre a moeda base (BRL) e as outras moedas suportadas.
            
            **DefiniÃ§Ã£o MatemÃ¡tica:**
            
            As taxas sÃ£o definidas como a quantidade de moeda estrangeira equivalente a 1 Real Brasileiro:
            ```
            1 BRL = Taxa_USD USD
            1 BRL = Taxa_EUR EUR
            ```
            
            **Exemplo PrÃ¡tico:**
            
            Se a taxa de cÃ¢mbio USD for 0,20, isso significa que:
            - 1 Real Brasileiro = 0,20 DÃ³lares Americanos
            - Para converter R$ 100,00 para USD: R$ 100,00 * 0,20 = $ 20,00
            
            **FÃ³rmula de ConversÃ£o:**
            
            Para converter um valor de BRL para outra moeda:
            ```
            Valor_Convertido = Valor_BRL * Taxa_Cambio
            ```
            
            Onde:
            - `Valor_BRL` = Valor original em Real Brasileiro
            - `Taxa_CÃ¢mbio` = Taxa de cÃ¢mbio da moeda de destino
            - `Valor_Convertido` = Valor convertido para a moeda de destino
            
            **Ordem de AplicaÃ§Ã£o das TransformaÃ§Ãµes:**
            
            Quando mÃºltiplas transformaÃ§Ãµes sÃ£o aplicadas (fator de conversÃ£o K/M e conversÃ£o de moeda), a ordem
            Ã© crÃ­tica para garantir resultados corretos:
            
            1. **Primeiro:** Aplicar fator de conversÃ£o (K/M) - apenas em modo Custo Total
            2. **Segundo:** Converter moeda (se necessÃ¡rio)
            3. **Terceiro:** Realizar cÃ¡lculos (CPU, Flex Bud, diferenÃ§as, etc.)
            
            **Exemplo Completo de TransformaÃ§Ã£o:**
            
            Considere um valor original de R$ 1.000.000,00:
            
            - **Passo 1 (Fator K):** R$ 1.000.000,00 / 1.000 = R$ 1.000 K
            - **Passo 2 (ConversÃ£o USD, taxa 0,20):** R$ 1.000 K * 0,20 = $ 200 K
            - **Resultado Final:** $ 200 K (duzentos mil dÃ³lares)
            """)
        
        with st.expander("ðŸ’¾ **PersistÃªncia e AtualizaÃ§Ã£o de Taxas**", expanded=False):
            st.markdown("""
            ### ðŸ’¾ PersistÃªncia e AtualizaÃ§Ã£o de Taxas
            
            As taxas de cÃ¢mbio sÃ£o armazenadas de forma persistente para garantir que as conversÃµes sejam
            consistentes entre diferentes sessÃµes de anÃ¡lise.
            
            **Armazenamento:**
            
            - As taxas sÃ£o salvas em banco de dados ou arquivo de configuraÃ§Ã£o
            - Valores padrÃ£o sÃ£o utilizados caso nÃ£o existam taxas salvas
            - As taxas podem ser atualizadas a qualquer momento atravÃ©s da interface do sistema
            
            **AtualizaÃ§Ã£o de Taxas:**
            
            As taxas de cÃ¢mbio podem ser atualizadas para refletir as condiÃ§Ãµes de mercado atuais. Quando uma
            nova taxa Ã© definida, ela Ã© aplicada a todos os cÃ¡lculos subsequentes, garantindo que as anÃ¡lises
            estejam sempre baseadas nas taxas mais recentes.
            
            **ImportÃ¢ncia da AtualizaÃ§Ã£o:**
            
            Manter as taxas de cÃ¢mbio atualizadas Ã© essencial para garantir a precisÃ£o das anÃ¡lises, especialmente
            em perÃ­odos de alta volatilidade cambial. Taxas desatualizadas podem resultar em comparaÃ§Ãµes e
            anÃ¡lises completamente incorretas.
            """)
    
    # EXPANDER 5: Filtros e PerÃ­metros
    with st.expander("ðŸ” **Filtros e PerÃ­metros de AnÃ¡lise**", expanded=False):
        with st.expander("ðŸŽ¯ **Sistema de Filtros da Interface**", expanded=False):
            st.markdown("""
            ### ðŸŽ¯ Sistema de Filtros da Interface
            
            O sistema possui um conjunto abrangente de filtros que permitem refinar a anÃ¡lise de dados de forma
            precisa e flexÃ­vel. Os filtros sÃ£o aplicados sequencialmente, criando um perÃ­metro de anÃ¡lise cada vez
            mais especÃ­fico conforme o usuÃ¡rio seleciona diferentes critÃ©rios.
            
            **Ordem de AplicaÃ§Ã£o dos Filtros:**
            
            Os filtros sÃ£o aplicados na seguinte ordem hierÃ¡rquica, garantindo que cada filtro refine o resultado
            do filtro anterior:
            
            1. **Ano** - SeleÃ§Ã£o do ano de anÃ¡lise (Radio button)
            2. **Oficina** - SeleÃ§Ã£o de uma ou mais oficinas (Multiselect)
            3. **VeÃ­culo** - SeleÃ§Ã£o de um ou mais veÃ­culos (Multiselect)
            4. **USI** - SeleÃ§Ã£o de unidades de serviÃ§o (Multiselect)
            5. **PerÃ­odo** - SeleÃ§Ã£o de perÃ­odo especÃ­fico (Selectbox)
            6. **Centro cst** - SeleÃ§Ã£o de centro de custo (Selectbox)
            7. **Conta contÃ¡bil** - SeleÃ§Ã£o de contas contÃ¡beis (Multiselect)
            8. **Type 5** - SeleÃ§Ã£o de categorias Type 05 (Multiselect)
            9. **Type 6** - SeleÃ§Ã£o de categorias Type 06 (Multiselect)
            10. **Fornecedor** - SeleÃ§Ã£o de fornecedores (Multiselect)
            11. **Fornec.** - SeleÃ§Ã£o adicional de fornecedores (Multiselect)
            12. **Tipo** - SeleÃ§Ã£o de tipos de custo (Multiselect)
            13. **Filtros AvanÃ§ados:**
                - **UsuÃ¡rio** - Filtro por usuÃ¡rio responsÃ¡vel
                - **Material** - Filtro por material utilizado
                - **Dt.lÃ§to.** - Filtro por data de lanÃ§amento
                - **Texto breve** - Filtro por texto descritivo
                - **Account** - Filtro por conta contÃ¡bil especÃ­fica
            
            **PrincÃ­pio de Funcionamento:**
            
            Cada filtro atua como um "funil" que reduz progressivamente o conjunto de dados analisados. Quando
            mÃºltiplos filtros sÃ£o aplicados, apenas os registros que atendem a **TODOS** os critÃ©rios selecionados
            sÃ£o incluÃ­dos na anÃ¡lise final.
            
            **Exemplo de AplicaÃ§Ã£o Sequencial:**
            
            Imagine que vocÃª selecionou:
            - Ano: 2024
            - Oficina: "Oficina A" e "Oficina B"
            - VeÃ­culo: "VeÃ­culo X"
            - PerÃ­odo: "Janeiro"
            
            O sistema primeiro filtra todos os dados de 2024, depois mantÃ©m apenas os registros das Oficinas A e B,
            em seguida mantÃ©m apenas os registros do VeÃ­culo X, e finalmente mantÃ©m apenas os registros de Janeiro.
            O resultado final contÃ©m apenas os registros que atendem a todos esses critÃ©rios simultaneamente.
            """)
        
        with st.expander("âš ï¸ **REGRA CRÃTICA: PerÃ­metro de Filtros para Volumes**", expanded=False):
            st.markdown("""
            ### âš ï¸ REGRA CRÃTICA: PerÃ­metro de Filtros para Volumes
            
            **PrincÃ­pio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta Ã© uma das regras mais importantes do sistema, pois garante que o cÃ¡lculo de
            CPU seja preciso e consistente.
            
            **Por que esta regra Ã© crÃ­tica?**
            
            O CPU Ã© calculado como a razÃ£o entre Custo Total e Volume. Se os filtros aplicados ao custo forem
            diferentes dos filtros aplicados ao volume, o CPU resultante serÃ¡ completamente incorreto.
            
            **Exemplo Ilustrativo:**
            
            Imagine que vocÃª filtrou os dados de custo para incluir apenas:
            - Oficina: "Oficina A"
            - VeÃ­culo: "VeÃ­culo X"
            - PerÃ­odo: "Janeiro"
            
            Se o volume nÃ£o for filtrado da mesma forma, vocÃª poderia estar dividindo:
            - Custo Total (filtrado): R$ 100.000 (apenas Oficina A, VeÃ­culo X, Janeiro)
            - Volume Total (nÃ£o filtrado): 50.000 unidades (todas as oficinas, todos os veÃ­culos, todos os perÃ­odos)
            - CPU Incorreto: R$ 100.000 / 50.000 = R$ 2,00/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 100.000 (Oficina A, VeÃ­culo X, Janeiro)
            - Volume Total (filtrado): 10.000 unidades (Oficina A, VeÃ­culo X, Janeiro)
            - CPU Correto: R$ 100.000 / 10.000 = R$ 10,00/unidade [CORRETO]
            
            **Mecanismo de SincronizaÃ§Ã£o:**
            
            O sistema garante a sincronizaÃ§Ã£o dos filtros extraindo os valores Ãºnicos das dimensÃµes filtradas dos
            dados principais e aplicando esses mesmos valores aos dados de volume. Isso garante que o perÃ­metro de
            anÃ¡lise seja idÃªntico para ambos os conjuntos de dados.
            
            **DimensÃµes Sincronizadas:**
            
            As seguintes dimensÃµes sÃ£o sempre sincronizadas entre dados de custo e volume:
            - VeÃ­culo
            - Oficina
            - USI
            - Centro de Custo
            - Conta ContÃ¡bil
            - Type 05
            - Type 06
            - Fornecedor
            - Tipo
            - E todos os filtros avanÃ§ados (UsuÃ¡rio, Material, Data, etc.)
            """)
        
        with st.expander("ðŸ“Š **SincronizaÃ§Ã£o de Filtros para Budget**", expanded=False):
            st.markdown("""
            ### ðŸ“Š SincronizaÃ§Ã£o de Filtros para Budget
            
            **Regra Fundamental:** O Budget deve usar os mesmos filtros aplicados aos dados reais para garantir
            comparaÃ§Ãµes justas e precisas.
            
            **Por que sincronizar filtros do Budget?**
            
            Quando comparamos dados reais com budget, precisamos garantir que estamos comparando "maÃ§Ã£s com maÃ§Ã£s".
            Se os dados reais estÃ£o filtrados para uma oficina especÃ­fica, o budget tambÃ©m deve estar filtrado para
            a mesma oficina, caso contrÃ¡rio a comparaÃ§Ã£o nÃ£o terÃ¡ sentido.
            
            **Exemplo:**
            
            Se vocÃª filtrar os dados reais para:
            - Oficina: "Oficina A"
            - VeÃ­culo: "VeÃ­culo X"
            
            O budget tambÃ©m serÃ¡ automaticamente filtrado para:
            - Oficina: "Oficina A"
            - VeÃ­culo: "VeÃ­culo X"
            
            Isso garante que a comparaÃ§Ã£o entre Real e Budget seja feita no mesmo contexto operacional.
            
            **Mecanismo de AplicaÃ§Ã£o:**
            
            O sistema extrai os valores Ãºnicos de todas as dimensÃµes filtradas dos dados reais e aplica esses mesmos
            valores como filtros ao budget. Isso garante que o perÃ­metro de anÃ¡lise seja idÃªntico para ambos os
            conjuntos de dados, permitindo comparaÃ§Ãµes precisas e significativas.
            """)

    # EXPANDER 3: Volumes
    with st.expander("ðŸ“Š **CÃ¡lculo de Volumes**", expanded=False):
        with st.expander("ðŸ“ **Fonte de Dados de Volume**", expanded=False):
            st.markdown("""
            ### ðŸ“ Fonte de Dados de Volume
            
            Os dados de volume sÃ£o armazenados em arquivos Parquet otimizados para garantir performance e eficiÃªncia
            no processamento. O sistema utiliza diferentes arquivos de volume dependendo do contexto de anÃ¡lise.
            
            **Arquivos de Volume:**
            
            - **`df_vol_historico.parquet`:** Dados histÃ³ricos consolidados de volume
            - **`df_vol.parquet`:** Dados de volume por ano especÃ­fico
            - **`df_vol_historico_BUD.parquet`:** Dados de volume do budget histÃ³rico
            
            **Estrutura dos Dados de Volume:**
            
            Os arquivos de volume contÃªm as seguintes colunas obrigatÃ³rias:
            - **`Volume`:** Quantidade de unidades produzidas
            - **`PerÃ­odo`:** PerÃ­odo de referÃªncia (mÃªs, trimestre, etc.)
            - **`Oficina`:** IdentificaÃ§Ã£o da oficina
            - **`VeÃ­culo`:** IdentificaÃ§Ã£o do veÃ­culo
            
            E a seguinte coluna opcional:
            - **`Ano`:** Ano de referÃªncia (quando disponÃ­vel)
            
            **SincronizaÃ§Ã£o com Dados de Custo:**
            
            Os dados de volume sÃ£o estruturados de forma a permitir sincronizaÃ§Ã£o perfeita com os dados de custo,
            garantindo que o cÃ¡lculo de CPU seja preciso e consistente.
            """)
        
        with st.expander("âš ï¸ **REGRA CRÃTICA: Filtragem de Volumes**", expanded=False):
            st.markdown("""
            ### âš ï¸ REGRA CRÃTICA: Filtragem de Volumes
            
            **PrincÃ­pio Fundamental:** Os volumes devem usar **EXATAMENTE** os mesmos filtros aplicados aos dados
            principais de custo. Esta regra Ã© absolutamente crÃ­tica para garantir a precisÃ£o do cÃ¡lculo de CPU.
            
            **Processo de Filtragem:**
            
            O processo de filtragem de volumes segue os seguintes passos:
            
            1. **Criar conjunto filtrado:** Iniciar com uma cÃ³pia dos dados de volume originais
            2. **Aplicar filtros sincronizados:** Usar os valores Ãºnicos extraÃ­dos dos dados de custo filtrados
            3. **Filtrar por perÃ­odo:** Aplicar filtro especÃ­fico para o perÃ­odo de anÃ¡lise
            4. **Agrupar e somar:** Agrupar por perÃ­odo e somar os volumes
            
            **FÃ³rmula de AgregaÃ§Ã£o:**
            
            ```
            Volume_Total = Î£(Volume_Individual) agrupado por PerÃ­odo
            ```
            
            Onde `Î£` representa a soma de todos os volumes individuais que atendem aos critÃ©rios de filtragem.
            
            **ImportÃ¢ncia da ConsistÃªncia:**
            
            A consistÃªncia entre os filtros aplicados aos dados de custo e volume Ã© essencial porque:
            - O CPU Ã© calculado como Custo Total / Volume Total
            - Se os filtros forem diferentes, o CPU resultante serÃ¡ completamente incorreto
            - AnÃ¡lises baseadas em CPU incorreto podem levar a decisÃµes de negÃ³cio equivocadas
            
            **Exemplo de Impacto:**
            
            Se vocÃª filtrar os custos para uma oficina especÃ­fica mas nÃ£o filtrar os volumes da mesma forma:
            - Custo Total (filtrado): R$ 50.000 (apenas Oficina A)
            - Volume Total (nÃ£o filtrado): 100.000 unidades (todas as oficinas)
            - CPU Incorreto: R$ 50.000 / 100.000 = R$ 0,50/unidade [INCORRETO]
            
            O correto seria:
            - Custo Total (filtrado): R$ 50.000 (Oficina A)
            - Volume Total (filtrado): 10.000 unidades (Oficina A)
            - CPU Correto: R$ 50.000 / 10.000 = R$ 5,00/unidade [CORRETO]
            """)
    
    st.markdown("---")
    
    st.markdown("## ðŸ“‹ Regras Fundamentais: Fixo vs NÃ£oâ€‘Fixo")
    
    st.markdown("""
    ### Regra Geral para Custos Fixos
    
    **PrincÃ­pio:** Custos fixos NÃƒO variam com o volume de produÃ§Ã£o.
    
    **FÃ³rmula Geral:**
    ```
    Flex_Fixo = Valor_Original_Fixo
    ```
    
    **ExplicaÃ§Ã£o:**
    - Independente da variaÃ§Ã£o de volume, o custo fixo permanece constante
    - Exemplos: Aluguel, salÃ¡rios fixos, depreciaÃ§Ã£o
    - Sensibilidade ao volume: **0%** (zero por cento)
    
    **ImplementaÃ§Ã£o:**
    ```python
    # Sempre manter o valor original
    flex_fixo = custo_fixo_original
    ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Regra Geral para Custos VariÃ¡veis
    
    **PrincÃ­pio:** Custos variÃ¡veis variam PROPORCIONALMENTE ao volume de produÃ§Ã£o.
    
    **FÃ³rmula Geral:**
    ```
    Flex_NÃ£oFixo = Valor_Original_NÃ£oFixo * (Volume_Novo / Volume_Original)
    ```
    
    **ExplicaÃ§Ã£o:**
    - Se o volume dobra, o custo variÃ¡vel dobra
    - Se o volume reduz pela metade, o custo variÃ¡vel reduz pela metade
    - Exemplos: MatÃ©ria-prima, energia variÃ¡vel, comissÃµes
    - Sensibilidade ao volume: **100%** (cem por cento)
    
    **ImplementaÃ§Ã£o:**
    ```python
    # Calcular proporÃ§Ã£o de volume
    proporcao = volume_novo / volume_original
    
    # Aplicar proporÃ§Ã£o ao custo variÃ¡vel
    flex_variavel = custo_variavel_original * proporcao
    ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### IdentificaÃ§Ã£o de Fixo vs VariÃ¡vel
    
    **Coluna 'Custo' no DataFrame:**
    - Deve conter os valores: `'Fixo'` ou `'VariÃ¡vel'`
    - Cada linha de dados deve ter esta classificaÃ§Ã£o
    
    **ImplementaÃ§Ã£o:**
    ```python
    # Separar Fixo e VariÃ¡vel
    if 'Custo' in df.columns:
        custo_fixo = df[df['Custo'] == 'Fixo']['Total'].sum()
        custo_variavel = df[df['Custo'] == 'VariÃ¡vel']['Total'].sum()
    else:
        # Se nÃ£o tiver coluna Custo, assumir tudo como variÃ¡vel
        custo_fixo = 0
        custo_variavel = df['Total'].sum()
    ```
    """)
    
    st.markdown("---")
    
    # Sub-seÃ§Ãµes para separar os dois casos
    st.markdown("## ðŸ“Š CASO 1: Flex para ComparaÃ§Ã£o Real x Real (Waterfall)")
    
    st.markdown("""
    ### Contexto
    
    Usado na pÃ¡gina **1 - Waterfall** para comparar dois perÃ­odos reais:
    - **MÃªs 1** (perÃ­odo inicial real)
    - **MÃªs 2** (perÃ­odo final real)
    
    **Objetivo:** Calcular o que seria o custo do MÃªs 1 ajustado pelo volume do MÃªs 2.
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Regras de CÃ¡lculo - Real x Real
        
        **Passo 1: Identificar Custos do MÃªs 1**
        ```python
        # Separar Fixo e VariÃ¡vel do MÃªs 1
        C1_Fixo = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum()
        C1_Variavel = df_m1[df_m1['Custo'] == 'VariÃ¡vel']['Total'].sum()
        C1_Total = C1_Fixo + C1_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V1 = volume_real_mes1  # Volume do MÃªs 1
        V2 = volume_real_mes2  # Volume do MÃªs 2
        ```
        
        **Passo 3: Calcular ProporÃ§Ã£o de Volume**
        ```python
        rho = V2 / V1  # ProporÃ§Ã£o de volume
        ```
        
        **Passo 4: Aplicar Regras de Fixo e VariÃ¡vel**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo nÃ£o varia com volume
        Flex_Mes1_Fixo = C1_Fixo
        # ExplicaÃ§Ã£o: MantÃ©m o valor original, independente da variaÃ§Ã£o de volume
        ```
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Flex_MÃªs1_Fixo = C_1_Fixo
        ```
        **Por que nÃ£o multiplica pela proporÃ§Ã£o?**
        - Custos fixos sÃ£o independentes do volume de produÃ§Ã£o
        - Exemplos: Aluguel, salÃ¡rios fixos, depreciaÃ§Ã£o
        - Mesmo que o volume dobre, o custo fixo permanece igual
        
        **Para Custo VariÃ¡vel:**
        ```python
        # REGRA: VariÃ¡vel varia proporcionalmente ao volume
        Flex_Mes1_Variavel = C1_Variavel * rho
                             = C1_Variavel * (V2 / V1)
        # ExplicaÃ§Ã£o: Multiplica pelo fator de proporÃ§Ã£o de volume
        ```
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Flex_MÃªs1_VariÃ¡vel = C_1_VariÃ¡vel * rho
                              = C_1_VariÃ¡vel * (V_2 / V_1)
        ```
        **Por que multiplica pela proporÃ§Ã£o?**
        - Custos variÃ¡veis variam proporcionalmente ao volume
        - Se o volume dobra, o custo variÃ¡vel dobra
        - Se o volume reduz pela metade, o custo variÃ¡vel reduz pela metade
        - Exemplos: MatÃ©ria-prima, energia variÃ¡vel, comissÃµes
        
        **Passo 5: Calcular Flex MÃªs 1 Total**
        ```python
        Flex_Mes1_Total = Flex_Mes1_Fixo + Flex_Mes1_Variavel
                         = C1_Fixo + (C1_Variavel * rho)
        ```
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Flex_MÃªs1_Total = Flex_MÃªs1_Fixo + Flex_MÃªs1_VariÃ¡vel
                        = C_1_Fixo + (C_1_VariÃ¡vel * rho)
                        = C_1_Fixo + C_1_VariÃ¡vel * (V_2 / V_1)
        ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### FÃ³rmulas MatemÃ¡ticas Completas - Real x Real
    
    **DefiniÃ§Ãµes:**
    - `V_1` = Volume Real do MÃªs 1
    - `V_2` = Volume Real do MÃªs 2
    - `C_1_Fixo` = Custo Total Fixo do MÃªs 1
    - `C_1_VariÃ¡vel` = Custo Total VariÃ¡vel do MÃªs 1
    - `C_1_Total` = Custo Total do MÃªs 1 = `C_1_Fixo + C_1_VariÃ¡vel`
    
    **ProporÃ§Ã£o de Volume:**
    ```
    rho = V_2 / V_1
    ```
    Onde:
    - `rho > 1` significa que o volume aumentou
    - `rho < 1` significa que o volume diminuiu
    - `rho = 1` significa que o volume permaneceu igual
    
    **CÃ¡lculo de Flex MÃªs 1 (em Custo Total):**
    
    Para **Custo Fixo:**
    ```
    Flex_MÃªs1_Fixo = C_1_Fixo
    ```
    **Regra Aplicada:** Fixo nÃ£o varia com volume
    - Valor original mantido: `C_1_Fixo`
    - NÃ£o multiplica pela proporÃ§Ã£o de volume
    - Motivo: Custos fixos sÃ£o independentes do volume de produÃ§Ã£o
    
    Para **Custo VariÃ¡vel:**
    ```
    Flex_MÃªs1_VariÃ¡vel = C_1_VariÃ¡vel * rho
                          = C_1_VariÃ¡vel * (V_2 / V_1)
    ```
    **Regra Aplicada:** VariÃ¡vel varia proporcionalmente ao volume
    - Valor original: `C_1_VariÃ¡vel`
    - Multiplica pela proporÃ§Ã£o: `rho = V_2 / V_1`
    - Motivo: Custos variÃ¡veis aumentam/diminuem na mesma proporÃ§Ã£o do volume
    
    **Flex MÃªs 1 Total (em Custo Total):**
    ```
    Flex_MÃªs1_Total = Flex_MÃªs1_Fixo + Flex_MÃªs1_VariÃ¡vel
                    = C_1_Fixo + (C_1_VariÃ¡vel * rho)
                    = C_1_Fixo + C_1_VariÃ¡vel * (V_2 / V_1)
    ```
    **Regra Aplicada:** Soma do Fixo (inalterado) + VariÃ¡vel (ajustado)
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### CÃ¡lculo em CPU (Custo por Unidade) - Real x Real
    
    **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD (MÃªs 1) em CPU:**
        ```
        BUD_CPU = C_1_Total / V_1
                 = (C_1_Fixo + C_1_VariÃ¡vel) / V_1
        ```
        
        **Flex MÃªs 1 em CPU:**
        ```
        Flex_MÃªs1_CPU = Flex_MÃªs1_Total / V_2
                       = [C_1_Fixo + C_1_VariÃ¡vel * (V_2 / V_1)] / V_2
                       = (C_1_Fixo / V_2) + (C_1_VariÃ¡vel / V_1)
        ```
        
        **DiferenÃ§a (Flex MÃªs 1 - MÃªs 1):**
        ```
        Delta_Flex = Flex_MÃªs1_CPU - BUD_CPU
               = [(C_1_Fixo / V_2) + (C_1_VariÃ¡vel / V_1)] - [(C_1_Fixo + C_1_VariÃ¡vel) / V_1]
               = (C_1_Fixo / V_2) - (C_1_Fixo / V_1)
               = C_1_Fixo * (1/V_2 - 1/V_1)
               = C_1_Fixo * (V_1 - V_2) / (V_1 * V_2)
        ```
        
        **InterpretaÃ§Ã£o:**
        - Se `V_2 > V_1`: `Delta_Flex < 0` (CPU diminui porque custo fixo Ã© diluÃ­do em mais volume)
        - Se `V_2 < V_1`: `Delta_Flex > 0` (CPU aumenta porque custo fixo Ã© concentrado em menos volume)
        - Se `V_2 = V_1`: `Delta_Flex = 0` (sem variaÃ§Ã£o)
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### ImplementaÃ§Ã£o - Real x Real
        
        ```python
        # 1. Obter dados do MÃªs 1
        df_m1 = df_filtrado[df_filtrado['PerÃ­odo'] == mes_inicial]
        
        # 2. Separar Fixo e VariÃ¡vel
        if 'Custo' in df_m1.columns:
            C1_Fixo = df_m1[df_m1['Custo'] == 'Fixo']['Total'].sum()
            C1_Variavel = df_m1[df_m1['Custo'] == 'VariÃ¡vel']['Total'].sum()
        else:
            C1_Fixo = 0
            C1_Variavel = df_m1['Total'].sum()  # Tudo Ã© variÃ¡vel
        
        C1_Total = C1_Fixo + C1_Variavel
        
        # 3. Obter volumes
        volume_m1 = df_vol_m1['Volume'].sum()
        volume_m2 = df_vol_m2['Volume'].sum()
        
        # 4. Calcular proporÃ§Ã£o
        rho = volume_m2 / volume_m1 if volume_m1 != 0 else 1.0
        
        # 5. Calcular Flex MÃªs 1 (em Custo Total)
        Flex_Mes1_Fixo = C1_Fixo  # NÃ£o varia
        Flex_Mes1_Variavel = C1_Variavel * rho  # Varia proporcionalmente
        Flex_Mes1_Total = Flex_Mes1_Fixo + Flex_Mes1_Variavel
        
        # 6. Converter para CPU (se necessÃ¡rio)
        if tipo_visualizacao == "CPU (Custo por Unidade)":
            BUD_CPU = C1_Total / volume_m1 if volume_m1 != 0 else 0
            Flex_Mes1_CPU = Flex_Mes1_Total / volume_m2 if volume_m2 != 0 else 0
            Delta_Flex = Flex_Mes1_CPU - BUD_CPU
        ```
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Exemplo PrÃ¡tico - Real x Real
        
        **Dados:**
        - Volume Real MÃªs 1 (`V_1`): 40,848 unidades
        - Volume Real MÃªs 2 (`V_2`): 60,333 unidades
        - Custo Total Fixo MÃªs 1 (`C_1_Fixo`): R$ 126.91
        - Custo Total VariÃ¡vel MÃªs 1 (`C_1_VariÃ¡vel`): R$ 755.36
        - Custo Total MÃªs 1 (`C_1_Total`): R$ 882.27
        
        **CÃ¡lculo:**
        ```
        rho = V_2 / V_1 = 60,333 / 40,848 = 1.482373
        
        Flex_MÃªs1_Fixo = R$ 126.91
        Flex_MÃªs1_VariÃ¡vel = R$ 755.36 * 1.482373 = R$ 1,119.72
        Flex_MÃªs1_Total = R$ 126.91 + R$ 1,119.72 = R$ 1,246.63
        ```
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 882.27 / 40,848 = R$ 0.0216 por unidade
        Flex_MÃªs1_CPU = R$ 1,246.63 / 60,333 = R$ 0.0207 por unidade
        Delta_Flex = R$ 0.0207 - R$ 0.0216 = -R$ 0.0009 por unidade
        ```
        
        **InterpretaÃ§Ã£o:**
        - O volume aumentou 48.24% (`rho = 1.482373`)
        - O custo variÃ¡vel aumentou proporcionalmente: R$ 755.36 -> R$ 1,119.72
        - O custo fixo permaneceu igual: R$ 126.91
        - Em CPU, o custo por unidade diminuiu porque o custo fixo foi diluÃ­do em mais volume
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Modos de ComparaÃ§Ã£o - Real x Real
        
        **MÃªs a MÃªs:**
        - `V_1` = Volume do mÃªs inicial
        - `V_2` = Volume do mÃªs final
        
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
    
    st.markdown("## ðŸ’° CASO 2: Flex para ComparaÃ§Ã£o Real x Budget (TC Ext)")
    
    st.markdown("""
        ### Contexto
        
        Usado na pÃ¡gina **TC Ext** para comparar perÃ­odo real vs budget planejado:
        - **Real** = Dados reais do perÃ­odo
        - **Budget** = Dados planejados do perÃ­odo
        
        **Objetivo:** Calcular o que seria o budget ajustado pelo volume real.
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Regras de CÃ¡lculo - Real x Budget
        
        **Passo 1: Identificar Custos do Budget**
        ```python
        # Separar Fixo e VariÃ¡vel do Budget
        B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
        B_Variavel = df_budget[df_budget['Custo'] == 'VariÃ¡vel']['Total'].sum()
        B_Total = B_Fixo + B_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V_Budget = volume_budget  # Volume planejado no Budget
        V_Real = volume_real      # Volume real do perÃ­odo
        ```
        
        **Passo 3: Calcular ProporÃ§Ã£o de Volume**
        ```python
        rho = V_Real / V_Budget  # ProporÃ§Ã£o de volume real vs planejado
        ```
        
        **Passo 4: Aplicar Regras de Fixo e VariÃ¡vel**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo nÃ£o varia com volume
        Flex_Bud_Fixo = B_Fixo
        # ExplicaÃ§Ã£o: MantÃ©m o valor do budget, independente da variaÃ§Ã£o de volume
        ```
        
        **Para Custo VariÃ¡vel:**
        ```python
        # REGRA: VariÃ¡vel varia proporcionalmente ao volume
        Flex_Bud_Variavel = B_Variavel * rho
                            = B_Variavel * (V_Real / V_Budget)
        # ExplicaÃ§Ã£o: Ajusta o budget variÃ¡vel pela proporÃ§Ã£o de volume real vs planejado
        ```
        
        **Passo 5: Calcular Flex Bud Total**
        ```python
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
                        = B_Fixo + (B_Variavel * rho)
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### FÃ³rmulas MatemÃ¡ticas Completas - Real x Budget
        
        **DefiniÃ§Ãµes:**
        ```python
        # Separar Fixo e VariÃ¡vel do Budget
        B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
        B_Variavel = df_budget[df_budget['Custo'] == 'VariÃ¡vel']['Total'].sum()
        B_Total = B_Fixo + B_Variavel
        ```
        
        **Passo 2: Obter Volumes**
        ```python
        V_Budget = volume_budget  # Volume planejado no Budget
        V_Real = volume_real      # Volume real do perÃ­odo
        ```
        
        **Passo 3: Calcular ProporÃ§Ã£o de Volume**
        ```python
        rho = V_Real / V_Budget  # ProporÃ§Ã£o de volume real vs planejado
        ```
        
        **Passo 4: Aplicar Regras de Fixo e VariÃ¡vel**
        
        **Para Custo Fixo:**
        ```python
        # REGRA: Fixo nÃ£o varia com volume
        Flex_Bud_Fixo = B_Fixo
        # ExplicaÃ§Ã£o: MantÃ©m o valor do budget, independente da variaÃ§Ã£o de volume
        ```
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Flex_Bud_Fixo = B_Fixo
        ```
        **Por que nÃ£o multiplica pela proporÃ§Ã£o?**
        - Custos fixos sÃ£o independentes do volume de produÃ§Ã£o
        - O budget fixo foi planejado e nÃ£o deve ser ajustado
        - Exemplos: Aluguel, salÃ¡rios fixos, depreciaÃ§Ã£o
        - Mesmo que o volume real seja diferente do planejado, o custo fixo permanece igual
        
        **Para Custo VariÃ¡vel:**
        ```python
        # REGRA: VariÃ¡vel varia proporcionalmente ao volume
        Flex_Bud_Variavel = B_Variavel * rho
                            = B_Variavel * (V_Real / V_Budget)
        # ExplicaÃ§Ã£o: Ajusta o budget variÃ¡vel pela proporÃ§Ã£o de volume real vs planejado
        ```
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Flex_Bud_VariÃ¡vel = B_VariÃ¡vel * rho
                           = B_VariÃ¡vel * (V_Real / V_Budget)
        ```
        **Por que multiplica pela proporÃ§Ã£o?**
        - Custos variÃ¡veis variam proporcionalmente ao volume
        - Se o volume real for maior que o planejado, o custo variÃ¡vel deve aumentar
        - Se o volume real for menor que o planejado, o custo variÃ¡vel deve diminuir
        - Exemplos: MatÃ©ria-prima, energia variÃ¡vel, comissÃµes
        - O budget variÃ¡vel precisa ser ajustado para refletir o volume real
        
        **Passo 5: Calcular Flex Bud Total**
        ```python
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
                        = B_Fixo + (B_Variavel * rho)
        ```
        **FÃ³rmula MatemÃ¡tica:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_VariÃ¡vel
                       = B_Fixo + (B_VariÃ¡vel * rho)
                       = B_Fixo + B_VariÃ¡vel * (V_Real / V_Budget)
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### FÃ³rmulas MatemÃ¡ticas Completas - Real x Budget
        
        **DefiniÃ§Ãµes:**
        - `V_Real` = Volume Real do perÃ­odo
        - `V_Budget` = Volume Budget planejado do perÃ­odo
        - `B_Fixo` = Custo Total Fixo do Budget
        - `B_VariÃ¡vel` = Custo Total VariÃ¡vel do Budget
        - `B_Total` = Custo Total do Budget = `B_Fixo + B_VariÃ¡vel`
        - `R_Total` = Custo Total Real do perÃ­odo
        
        **ProporÃ§Ã£o de Volume:**
        ```
        rho = V_Real / V_Budget
        ```
        Onde:
        - `rho > 1` significa que o volume real foi maior que o planejado
        - `rho < 1` significa que o volume real foi menor que o planejado
        - `rho = 1` significa que o volume real foi exatamente o planejado
        
        **CÃ¡lculo de Flex Bud (em Custo Total):**
        
        Para **Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
        ```
        **Regra Aplicada:** Fixo nÃ£o varia com volume
        - Valor do budget mantido: `B_Fixo`
        - NÃ£o multiplica pela proporÃ§Ã£o de volume
        - Motivo: Custos fixos sÃ£o independentes do volume, entÃ£o mantÃ©m o valor planejado
        
        Para **Custo VariÃ¡vel:**
        ```
        Flex_Bud_VariÃ¡vel = B_VariÃ¡vel * rho
                           = B_VariÃ¡vel * (V_Real / V_Budget)
        ```
        **Regra Aplicada:** VariÃ¡vel varia proporcionalmente ao volume
        - Valor do budget: `B_VariÃ¡vel`
        - Multiplica pela proporÃ§Ã£o: `rho = V_Real / V_Budget`
        - Motivo: Se o volume real for maior que o planejado, o custo variÃ¡vel deve aumentar proporcionalmente
        
        **Flex Bud Total (em Custo Total):**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_VariÃ¡vel
                       = B_Fixo + (B_VariÃ¡vel * rho)
                       = B_Fixo + B_VariÃ¡vel * (V_Real / V_Budget)
        ```
        **Regra Aplicada:** Soma do Fixo (inalterado) + VariÃ¡vel (ajustado)
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### CÃ¡lculo em CPU (Custo por Unidade) - Real x Budget
        
        **IMPORTANTE:** No modo CPU, calcular em Custo Total primeiro, depois converter.
        
        **BUD em CPU:**
        ```
        BUD_CPU = B_Total / V_Budget
                 = (B_Fixo + B_VariÃ¡vel) / V_Budget
        ```
        
        **Flex Bud em CPU:**
        ```
        Flex_Bud_CPU = Flex_Bud_Total / V_Real
                     = [B_Fixo + B_VariÃ¡vel * (V_Real / V_Budget)] / V_Real
                     = (B_Fixo / V_Real) + (B_VariÃ¡vel / V_Budget)
        ```
        
        **Total Real em CPU:**
        ```
        Total_Real_CPU = R_Total / V_Real
        ```
        
        **DiferenÃ§as:**
        
        **Flex Bud - BUD:**
        ```
        Delta_Flex_Bud = Flex_Bud_CPU - BUD_CPU
                   = [(B_Fixo / V_Real) + (B_VariÃ¡vel / V_Budget)] - [(B_Fixo + B_VariÃ¡vel) / V_Budget]
                   = (B_Fixo / V_Real) - (B_Fixo / V_Budget)
                   = B_Fixo * (1/V_Real - 1/V_Budget)
                   = B_Fixo * (V_Budget - V_Real) / (V_Real * V_Budget)
        ```
        
        **Total - Flex Bud:**
        ```
        Delta_Total_Flex = Total_Real_CPU - Flex_Bud_CPU
                     = (R_Total / V_Real) - [(B_Fixo / V_Real) + (B_VariÃ¡vel / V_Budget)]
        ```
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### ImplementaÃ§Ã£o - Real x Budget
        
        ```python
        # 1. Obter dados de Budget
        df_budget = load_budget_data(ano_selecionado)
        
        # 2. Separar Fixo e VariÃ¡vel do Budget
        if 'Custo' in df_budget.columns:
            B_Fixo = df_budget[df_budget['Custo'] == 'Fixo']['Total'].sum()
            B_Variavel = df_budget[df_budget['Custo'] == 'VariÃ¡vel']['Total'].sum()
        else:
            B_Fixo = 0
            B_Variavel = df_budget['Total'].sum()  # Tudo Ã© variÃ¡vel
        
        B_Total = B_Fixo + B_Variavel
        
        # 3. Obter volumes
        volume_real = df_vol_real['Volume'].sum()
        volume_budget = df_vol_budget['Volume'].sum()
        
        # 4. Calcular proporÃ§Ã£o
        rho = volume_real / volume_budget if volume_budget != 0 else 1.0
        
        # 5. Calcular Flex Bud (em Custo Total)
        Flex_Bud_Fixo = B_Fixo  # NÃ£o varia
        Flex_Bud_Variavel = B_Variavel * rho  # Varia proporcionalmente
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_Variavel
        
        # 6. Converter para CPU (se necessÃ¡rio)
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
        ### Exemplo PrÃ¡tico - Real x Budget
        
        **Dados:**
        - Volume Real (`V_Real`): 50,000 unidades
        - Volume Budget (`V_Budget`): 60,000 unidades
        - Custo Total Fixo Budget (`B_Fixo`): R$ 200,000
        - Custo Total VariÃ¡vel Budget (`B_VariÃ¡vel`): R$ 400,000
        - Custo Total Budget (`B_Total`): R$ 600,000
        - Custo Total Real (`R_Total`): R$ 550,000
        
        **CÃ¡lculo Passo a Passo:**
        
        **1. Calcular ProporÃ§Ã£o de Volume:**
        ```
        rho = V_Real / V_Budget = 50,000 / 60,000 = 0.833333
        ```
        *InterpretaÃ§Ã£o: Volume real foi 16.67% menor que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo nÃ£o varia -> mantÃ©m valor do budget*
        
        **3. Aplicar Regra para Custo VariÃ¡vel:**
        ```
        Flex_Bud_VariÃ¡vel = B_VariÃ¡vel * rho
                           = R$ 400,000 * 0.833333
                           = R$ 333,333.33
        ```
        *Regra: VariÃ¡vel varia proporcionalmente -> ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_VariÃ¡vel
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
        
        **InterpretaÃ§Ã£o:**
        - O volume real foi 16.67% menor que o planejado (`rho = 0.833333`)
        - O budget variÃ¡vel foi ajustado proporcionalmente: R$ 400,000 -> R$ 333,333.33
        - O budget fixo permaneceu igual: R$ 200,000
        - Em CPU, o Flex Bud aumentou porque o custo fixo foi concentrado em menos volume
        - O Total Real estÃ¡ R$ 0.33 acima do Flex Bud, indicando ineficiÃªncia operacional
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Exemplo PrÃ¡tico 2 - Real x Budget (Volume Real > Volume Budget)
        
        **Dados:**
        - Volume Real (`V_Real`): 62,208 unidades
        - Volume Budget (`V_Budget`): 60,120 unidades
        - Custo Total Fixo Budget (`B_Fixo`): R$ 200,000
        - Custo Total VariÃ¡vel Budget (`B_VariÃ¡vel`): R$ 400,000
        - Custo Total Budget (`B_Total`): R$ 600,000
        
        **CÃ¡lculo Passo a Passo:**
        
        **1. Calcular ProporÃ§Ã£o de Volume:**
        ```
        rho = V_Real / V_Budget = 62,208 / 60,120 = 1.0347
        ```
        *InterpretaÃ§Ã£o: Volume real foi 3.47% maior que o planejado*
        
        **2. Aplicar Regra para Custo Fixo:**
        ```
        Flex_Bud_Fixo = B_Fixo
                       = R$ 200,000
        ```
        *Regra: Fixo nÃ£o varia -> mantÃ©m valor do budget*
        
        **3. Aplicar Regra para Custo VariÃ¡vel:**
        ```
        Flex_Bud_VariÃ¡vel = B_VariÃ¡vel * rho
                           = R$ 400,000 * 1.0347
                           = R$ 413,880
        ```
        *Regra: VariÃ¡vel varia proporcionalmente -> ajusta pelo volume real*
        
        **4. Calcular Total:**
        ```
        Flex_Bud_Total = Flex_Bud_Fixo + Flex_Bud_VariÃ¡vel
                        = R$ 200,000 + R$ 413,880
                        = R$ 613,880
        ```
        *Resultado: Flex_Bud_Total (R$ 613,880) > BUD_Total (R$ 600,000) [CORRETO]*
        
        **Em CPU:**
        ```
        BUD_CPU = R$ 600,000 / 60,120 = R$ 9.98 por unidade
        Flex_Bud_CPU = R$ 613,880 / 62,208 = R$ 9.87 por unidade
        ```
        
        **DiferenÃ§as:**
        ```
        Delta_Flex_Bud (Custo Total) = R$ 613,880 - R$ 600,000 = R$ 13,880 (positivo) [CORRETO]
        Delta_Flex_Bud (CPU) = R$ 9.87 - R$ 9.98 = -R$ 0.11 (negativo)
        ```
        
        **InterpretaÃ§Ã£o:**
        - O volume real foi 3.47% maior que o planejado (`rho = 1.0347`)
        - O budget variÃ¡vel foi ajustado proporcionalmente: R$ 400,000 -> R$ 413,880
        - O budget fixo permaneceu igual: R$ 200,000
        - **Em Custo Total:** Flex_Bud_Total > BUD_Total (porque o custo variÃ¡vel aumentou)
        - **Em CPU:** Flex_Bud_CPU < BUD_CPU (porque o custo fixo foi diluÃ­do em mais volume)
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### ComparaÃ§Ã£o: Real x Real vs Real x Budget
        
        | Aspecto | Real x Real (Waterfall) | Real x Budget (TC Ext) |
        |---------|------------------------|------------------------|
        | **Base** | Custo Real MÃªs 1 | Custo Budget |
        | **Volume ReferÃªncia** | Volume Real MÃªs 1 | Volume Budget |
        | **Volume Ajuste** | Volume Real MÃªs 2 | Volume Real |
        | **ProporÃ§Ã£o** | `V_2 / V_1` | `V_Real / V_Budget` |
        | **Objetivo** | Ajustar MÃªs 1 pelo volume do MÃªs 2 | Ajustar Budget pelo volume Real |
        | **Uso** | Comparar dois perÃ­odos reais | Comparar Real vs Planejado |
        """)
        
    st.markdown("---")
        
    st.markdown("""
        ### Regras Gerais AplicÃ¡veis a Ambos os Casos
        
        **1. Custo Fixo:**
        - Sempre mantÃ©m o valor original (nÃ£o varia com volume)
        - `Flex_Fixo = Valor_Original`
        
        **2. Custo VariÃ¡vel:**
        - Varia proporcionalmente ao volume
        - `Flex_VariÃ¡vel = Valor_Original * (Volume_Novo / Volume_Original)`
        
        **3. Ordem de CÃ¡lculo:**
        1. Calcular em **Custo Total** primeiro
        2. Separar Fixo e VariÃ¡vel
        3. Aplicar proporÃ§Ã£o de volume apenas ao VariÃ¡vel
        4. Somar Fixo + VariÃ¡vel ajustado
        5. Se necessÃ¡rio, converter para **CPU** dividindo pelo volume final
        
        **4. Tratamento de DivisÃ£o por Zero:**
        - Se `Volume_Original = 0`: usar `rho = 1.0` (sem ajuste)
        - Se `Volume_Final = 0`: usar `Flex_CPU = 0`
        """)

# ==========================================
# TC VEÃCULOS: CÃLCULO POR TABELAS/GRÃFICOS
# ==========================================
elif indice_selecionado == "ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos (Normal vs CPU)" and modulo_doc.startswith("ðŸ“Œ Ambos"):
    st.header("ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos (Normal vs CPU) â€” TC Ext + TC VeÃ­culos")

    st.subheader("ðŸ“Š TC Estendido")
    _caminho_ext = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    _md_ext, _err_ext, _mtime_ext = _carregar_markdown(_caminho_ext)
    if _err_ext:
        st.error(_err_ext)
    else:
        st.caption(
            f"Fonte: {_caminho_ext} | Atualizado em: {_formatar_mtime(_mtime_ext)}"
        )
        st.markdown(
            _extrair_secao_por_heading(
                _md_ext,
                [
                    "## 4) VisualizaÃ§Ãµes â€” TC Estendido",
                    "## 4) VisualizaÃ§Ãµes",
                ],
            )
        )

    st.markdown("---")

    st.subheader("ðŸš— TC VeÃ­culos")
    _caminho_veic = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
    _md_veic, _err_veic, _mtime_veic = _carregar_markdown(_caminho_veic)
    if _err_veic:
        st.error(_err_veic)
    else:
        st.caption(
            f"Fonte: {_caminho_veic} | Atualizado em: {_formatar_mtime(_mtime_veic)}"
        )
        st.markdown(
            _extrair_secao_por_heading(
                _md_veic,
                ["## 8) VisualizaÃ§Ãµes e GrÃ¡ficos"],
            )
        )

    st.stop()

elif indice_selecionado == "ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos (Normal vs CPU)" and modulo_doc == "ðŸš— TC VeÃ­culos":
    st.header("ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos â€” TC VeÃ­culos")

    _caminho = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
    _md, _err, _mtime = _carregar_markdown(_caminho)
    if _err:
        st.error(_err)
        st.stop()

    st.caption(f"Fonte: {_caminho} | Atualizado em: {_formatar_mtime(_mtime)}")
    st.markdown(_extrair_secao_por_heading(_md, ["## 8) VisualizaÃ§Ãµes e GrÃ¡ficos"]))
    st.stop()

    st.info(
        "ðŸ“Œ **MÃ³dulo TC VeÃ­culos** â€” Tabelas e grÃ¡ficos especÃ­ficos do TC VeÃ­culos."
    )

    with st.expander("ðŸ“Š **AnÃ¡lise Flex por Categoria**", expanded=True):
        st.markdown("""
        ### ðŸ” Modos de VisualizaÃ§Ã£o

        - **Fixo/VariÃ¡vel**: Expanders `ðŸ’° Fixo` e `ðŸ’° VariÃ¡vel`, cada um com sub-expanders por `Type 05` â†’ tabela por `Account`
        - **Total**: Expanders direto por `Type 05` â†’ tabela por `Account`

        **Expander TOTAL:**
        - Re-agrega **todas** as linhas das oficinas por `(Type 05, Type 06, Account, Custo)`
        - Mostra tabela detalhada com todas as contas (nÃ£o apenas 1 linha sintÃ©tica)
        - Mesmo layout dos expanders por oficina

        ### ðŸ“‹ Tabela Flex por Account

        | Coluna | CÃ¡lculo |
        |--------|---------|
        | Account | Nome da conta |
        | BUD | Î£ Custo FP Budget |
        | Flex Bud âˆ’ BUD | Flex âˆ’ BUD |
        | Flex BUD | Fixo: BUD / VariÃ¡vel: BUD Ã— ProporÃ§Ã£o |
        | Total âˆ’ Flex Bud | Real âˆ’ Flex |
        | Total | Î£ Custo FP Real |
        | Total / Flex Bud | Real/Flex (com barrinha de progresso) |

        ### ðŸŽ¨ Barrinha de Progresso
        - ðŸŸ¢ Verde: â‰¤ 90%
        - ðŸŸ¡ Gradiente verdeâ†’vermelho: 90%â€“100%
        - ðŸ”´ Vermelho: â‰¥ 100%
        """)

    with st.expander("ðŸ“ˆ **GrÃ¡ficos do TC VeÃ­culos**", expanded=False):
        st.markdown("""
        ### ðŸ“Š Custo FP por PerÃ­odo
        - **Barras**: Real por perÃ­odo com degradÃª roxo (`scheme='purples'`)
        - **Linha pontilhada**: Flex BUD (laranja, `strokeDash=[10,5]`)
        - **Delta**: GrÃ¡fico inferior com `Real âˆ’ Flex BUD` (verde/vermelho)
        - Biblioteca: **Altair** com `data_transformers.disable_max_rows()`

        ### ðŸŽ¨ Cores do Best Estimate
        Na pÃ¡gina de **AnÃ¡lise BE**, os grÃ¡ficos por perÃ­odo usam codificaÃ§Ã£o por cor
        na coluna `Tipo` para diferenciar meses:
        - ðŸŸ£ **Roxo escuro** (`#4C1D95`): meses **HistÃ³ricos** (realizados)
        - ðŸŸ£ **Roxo claro** (`#C4B5FD`): meses de **Best Estimate** (projetados)

        ### ðŸ“Š Volume
        - **Barras**: Volume Budget (degradÃª verde)
        - **Linha tracejada**: Volume Realizado (laranja)
        - **Por VeÃ­culo**: Barras agrupadas por modelo

        ### ðŸ“Š Custos por Oficina
        - Barras Custo FP por Oficina
        - Barras Rateio FA por Oficina (verde/vermelho)
        - Tabela BUD vs Flex pivotada Oficina Ã— PerÃ­odo
        """)

    with st.expander("ðŸ“‹ **Tabs DisponÃ­veis**", expanded=False):
        st.markdown("""
        ### ðŸ—‚ï¸ OrganizaÃ§Ã£o em Tabs

        O TC VeÃ­culos organiza os dados em **6 tabs**:

        | Tab | ConteÃºdo |
        |-----|----------|
        | ðŸš— TC VeÃ­culos | KPIs resumo + GrÃ¡fico Custo FP Ã— Flex BUD por perÃ­odo |
        | ðŸ“Š AnÃ¡lise Flex | Fixo/VariÃ¡vel com hierarquia Type 05 â†’ Account |
        | ðŸ“ˆ Volume | Budget vs Realizado (por perÃ­odo e por veÃ­culo) |
        | ðŸ¢ Custos por Oficina | Custo FP e Rateio FA por oficina |
        | â±ï¸ Tempo de ProduÃ§Ã£o | Tempo VeÃ­culo vs Tempo FA por oficina |
        | ðŸ“‹ Dados Detalhados | Tabelas exportÃ¡veis de Real e Budget |
        """)

# ==========================================
# SEÃ‡ÃƒO 2: CÃLCULO POR TABELAS/GRÃFICOS
# ==========================================
elif indice_selecionado == "ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos (Normal vs CPU)":
    st.header("ðŸ§® CÃ¡lculo por Tabelas/GrÃ¡ficos â€” TC Estendido")

    _caminho = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    _md, _err, _mtime = _carregar_markdown(_caminho)
    if _err:
        st.error(_err)
        st.stop()

    st.caption(f"Fonte: {_caminho} | Atualizado em: {_formatar_mtime(_mtime)}")
    st.markdown(
        "Esta seÃ§Ã£o explica os pontos que mais geram divergÃªncia entre **tabela** e **grÃ¡fico** "
        "no TC Ext (Normal vs CPU)."
    )

    with st.expander("ðŸ“Œ CPU e regra de agregaÃ§Ã£o", expanded=True):
        st.markdown(
            _extrair_secao_por_heading(
                _md,
                ["## 2) Regras e CÃ¡lculo â€” TC Estendido"],
            )
        )

    with st.expander("ðŸ“Œ GovernanÃ§a do ano completo (12 meses)", expanded=False):
        st.markdown(
            _extrair_secao_por_heading(
                _md,
                ["## 7) Flex Bud â€” Ano Completo e GovernanÃ§a"],
            )
        )
    st.stop()

    caminho_doc = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    if not os.path.exists(caminho_doc):
        st.error(f"Arquivo nÃ£o encontrado: {caminho_doc}")
    else:
        try:
            with open(caminho_doc, "r", encoding="utf-8") as f:
                conteudo = f.read()

            def _extrair_trecho(md: str) -> str:
                start_token = "### 9.6 Guia de cÃ¡lculo por visualizaÃ§Ã£o"
                start = md.find(start_token)
                if start == -1:
                    start_token = "## 9) GrÃ¡ficos e tabelas"
                    start = md.find(start_token)
                if start == -1:
                    return "âš ï¸ NÃ£o encontrei a seÃ§Ã£o de cÃ¡lculos no arquivo de especificaÃ§Ã£o."

                end = md.find("\n## ", start + 1)
                if end == -1:
                    end = len(md)
                return md[start:end].strip()

            st.markdown("---")
            st.markdown(_extrair_trecho(conteudo))
        except Exception as e:
            st.error(f"Erro ao carregar/parsear especificaÃ§Ã£o: {e}")

# ==========================================
# TC VEÃCULOS: ARQUITETURA E ESTRUTURA
# ==========================================
elif indice_selecionado == "ðŸ—ï¸ Arquitetura e Estrutura" and modulo_doc.startswith("ðŸ“Œ Ambos"):
    st.header("ðŸ—ï¸ Arquitetura e Estrutura â€” TC Ext + TC VeÃ­culos")

    st.subheader("ðŸ“Š TC Estendido")
    _caminho_ext = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    _md_ext, _err_ext, _mtime_ext = _carregar_markdown(_caminho_ext)
    if _err_ext:
        st.error(_err_ext)
    else:
        st.caption(
            f"Fonte: {_caminho_ext} | Atualizado em: {_formatar_mtime(_mtime_ext)}"
        )
        st.markdown(
            _extrair_secao_por_heading(
                _md_ext,
                ["## 3) Arquitetura â€” TC Estendido"],
            )
        )

    st.markdown("---")

    st.subheader("ðŸš— TC VeÃ­culos")
    _caminho_veic = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
    _md_veic, _err_veic, _mtime_veic = _carregar_markdown(_caminho_veic)
    if _err_veic:
        st.error(_err_veic)
    else:
        st.caption(
            f"Fonte: {_caminho_veic} | Atualizado em: {_formatar_mtime(_mtime_veic)}"
        )
        st.markdown(
            _extrair_secao_por_heading(
                _md_veic,
                ["## 10) Arquitetura TC VeÃ­culos"],
            )
        )

    st.stop()

elif indice_selecionado == "ðŸ—ï¸ Arquitetura e Estrutura" and modulo_doc == "ðŸš— TC VeÃ­culos":
    st.header("ðŸ—ï¸ Arquitetura e Estrutura â€” TC VeÃ­culos")

    _caminho = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
    _md, _err, _mtime = _carregar_markdown(_caminho)
    if _err:
        st.error(_err)
        st.stop()

    st.caption(f"Fonte: {_caminho} | Atualizado em: {_formatar_mtime(_mtime)}")
    st.markdown(_extrair_secao_por_heading(_md, ["## 10) Arquitetura TC VeÃ­culos"]))
    st.stop()

    st.info(
        "ðŸ“Œ **MÃ³dulo TC VeÃ­culos** â€” Estrutura de pastas, contratos de dados e pipeline de processamento."
    )

    with st.expander("ðŸ“ **Contratos de Dados (Parquets)**", expanded=True):
        st.markdown("""
        ### ðŸ“‚ Estrutura de Pastas

        ```
        dados/TC_Principal/
        â”œâ”€â”€ {ano}/
        â”‚   â”œâ”€â”€ BUD/
        â”‚   â”‚   â”œâ”€â”€ df_principal_BUD.parquet         # Custo consolidado BUD
        â”‚   â”‚   â”œâ”€â”€ df_vol_veiculos_BUD.parquet      # Volume por veÃ­culo BUD
        â”‚   â”‚   â”œâ”€â”€ df_veiculos_custo_fp_BUD.parquet  # Custo FP rateado BUD
        â”‚   â”‚   â”œâ”€â”€ df_veiculos_cpu_BUD.parquet      # CPU por veÃ­culo BUD
        â”‚   â”‚   â”œâ”€â”€ df_tempo_veiculos_BUD.parquet    # Tempo de produÃ§Ã£o BUD
        â”‚   â”‚   â”œâ”€â”€ df_dea_dedicado_BUD.parquet      # D&A Dedicado BUD
        â”‚   â”‚   â””â”€â”€ df_volume_fa_BUD.parquet         # Volume Fluxo Anexo BUD
        â”‚   â”œâ”€â”€ df_principal.parquet                 # Custo Real consolidado
        â”‚   â”œâ”€â”€ df_vol_veiculos_actual.parquet       # Volume Realizado
        â”‚   â”œâ”€â”€ df_veiculos_custo_fp.parquet         # Custo FP Real rateado
        â”‚   â””â”€â”€ df_veiculos_cpu.parquet              # CPU Real
        â”œâ”€â”€ Forecast/
        â”‚   â”œâ”€â”€ forecast_completo.parquet            # ProjeÃ§Ã£o BE mÃªs a mÃªs
        â”‚   â””â”€â”€ premissas.json                       # Premissas do simulador
        â””â”€â”€ historico_consolidado/
            â”œâ”€â”€ df_principal_historico.parquet        # Multi-ano consolidado
            â””â”€â”€ BUD/
                â””â”€â”€ df_principal_historico_BUD.parquet
        ```

        ### ðŸ“‹ Schema â€” Principal BUD

        | Coluna | Tipo | DescriÃ§Ã£o |
        |--------|------|-----------|
        | `Oficina` | str | Centro de custo (oficina) |
        | `VeÃ­culo` | str | Modelo do veÃ­culo |
        | `Type 05` | str | ClassificaÃ§Ã£o nÃ­vel 1 |
        | `Type 06` | str | ClassificaÃ§Ã£o nÃ­vel 2 |
        | `Custo` | str | Fixo ou VariÃ¡vel |
        | `Account` | str | Conta contÃ¡bil (inclui "Redis") |
        | `PerÃ­odo` | str | MÃªs por extenso |
        | `Despesa Primaria` | float | Despesa primÃ¡ria (R$) |
        | `Custo FA` | float | Custo do Fluxo Anexo |
        | `Custo FP` | float | Custo FP consolidado |
        | `D&A dedicado` | float | D&A dedicada |
        | `FP sem Dedicada` | float | Custo FP sem D&A |

        ### ðŸ“‹ Schema â€” VeÃ­culos Rateado (BUD)

        | Coluna | Tipo | DescriÃ§Ã£o |
        |--------|------|-----------|
        | `Oficina` | str | Centro de custo |
        | `VeÃ­culo` | str | Modelo do veÃ­culo |
        | `Custo Rateado` | float | Custo Ã— percentual do veÃ­culo |
        | `D&A dedicado` | float | D&A dedicada direta |
        | `Custo FP Veiculo` | float | Rateado + D&A |
        | `Ano` | int | Ano de referÃªncia |

        > O parquet BUD veÃ­culos tem `Custo FP Veiculo` (nÃ£o `Custo FP`). O sistema faz mapeamento automÃ¡tico.
        """)

    with st.expander("ðŸ”§ **MÃ³dulos e Arquivos**", expanded=False):
        st.markdown("""
        ### ðŸ“‚ Estrutura do CÃ³digo

        ```
        tc_principal/
        â”œâ”€â”€ __init__.py
        â”œâ”€â”€ shared.py              # Constantes, loaders, helpers, ratear_be_por_veiculo()
        â”œâ”€â”€ ui_components.py       # Sidebar filters, CSS, KPIs
        â””â”€â”€ pages/
            â”œâ”€â”€ __init__.py
            â”œâ”€â”€ home_tc.py                      # PÃ¡gina principal (6 tabs) + consumo/anÃ¡lise do Forecast (Real vs BE)
            â”œâ”€â”€ best_estimate_simulador_tc.py   # Simulador de premissas BE (gera Forecast)
            â””â”€â”€ waterfall_tc.py                 # AnÃ¡lise Waterfall (Real + Budget)
        ```

        ### âš™ï¸ Filtros â€” Arquitetura Unificada

        ```
        Sidebar filters
             â”‚
             â”œâ”€â”€ VeÃ­culo = "Todos" â”€â”€â–º usar_rateado = False
             â”‚         â”œâ”€â”€ df_principal_BUD  â†’ df_bud
             â”‚         â””â”€â”€ df_principal_Real â†’ df
             â”‚
             â””â”€â”€ VeÃ­culo = "CC21 biton" â”€â”€â–º usar_rateado = True
                       â”œâ”€â”€ df_veiculos_custo_fp_BUD â†’ df_bud (filtrado)
                       â””â”€â”€ df_veiculos_custo_fp_Real â†’ df (filtrado)
             â”‚
        aplicar_fator_df() + converter_moeda_df()
             â”‚
        calcular_flex_budget()
             â”‚
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Todos os tabs usam         â”‚
        â”‚  df_bud, df, df_vol_bud,    â”‚
        â”‚  df_vol_actual, df_flex     â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
        ```
        """)

    with st.expander("âš™ï¸ **ETL e Processamento**", expanded=False):
        st.markdown("""
        ### ðŸ“‹ Arquivos de Processamento

        | Arquivo | FunÃ§Ã£o |
        |---------|--------|
        | `tc_principal/pages/extracao_dados_tc.py` | Orquestra upload, prÃ©-validaÃ§Ã£o e execuÃ§Ã£o (Real/Budget) |
        | `processamento_dados_veiculos_BUD.py` | Processa Budget (BUD) e grava parquets BUD |
        | `processamento_dados_veiculos.py` | Processa Real (Sapiens/Redis) e grava parquets Real |

        ### ðŸ”„ Pipeline

        1. ExtraÃ§Ã£o dos dados brutos (Excel/SAP)
        2. NormalizaÃ§Ã£o de colunas e perÃ­odos
        3. CÃ¡lculo de composiÃ§Ã£o de custos (Desp. PrimÃ¡ria â†’ FA â†’ FP)
        4. Rateio por veÃ­culo (tempo de produÃ§Ã£o)
        5. CÃ¡lculo de CPU por veÃ­culo
        6. GravaÃ§Ã£o em Parquet na pasta `dados/TC_Principal/{ano}/`

        ### ðŸ’¾ Cache
        - `@st.cache_data(ttl=3600)` em todos os loaders
        - BotÃ£o "ðŸ”„ Limpar Cache" na sidebar para forÃ§ar recarga
        """)

    with st.expander("ðŸŒ **ConfiguraÃ§Ãµes Globais**", expanded=False):
        st.markdown("""
        ### ðŸ’± Moeda

        | CÃ³digo | SÃ­mbolo | ConversÃ£o |
        |--------|---------|-----------|
        | BRL | R$ | 1.0 (base) |
        | USD | $ | 1/Taxa USDâ†’BRL |
        | EUR | â‚¬ | 1/Taxa EURâ†’BRL |

        ### ðŸ“Š Fator

        | OpÃ§Ã£o | Divisor |
        |-------|---------|
        | Nenhum | 1 |
        | K (milhares) | 1.000 |
        | M (milhÃµes) | 1.000.000 |

        ### ðŸ‘ï¸ Tipo de VisualizaÃ§Ã£o

        | Tipo | Comportamento |
        |------|---------------|
        | Custo Total | Valores absolutos em R$/USD/EUR |
        | CPU | Custo Ã· Volume (fator = Nenhum) |
        """)

# ==========================================
# SEÃ‡ÃƒO 2: ARQUITETURA E ESTRUTURA
# ==========================================
elif indice_selecionado == "ðŸ—ï¸ Arquitetura e Estrutura":
    st.header("ðŸ—ï¸ Arquitetura e Estrutura â€” TC Estendido")

    _caminho = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
    _md, _err, _mtime = _carregar_markdown(_caminho)
    if _err:
        st.error(_err)
        st.stop()

    st.caption(f"Fonte: {_caminho} | Atualizado em: {_formatar_mtime(_mtime)}")
    st.markdown(_extrair_secao_por_heading(_md, ["## 3) Arquitetura â€” TC Estendido"]))
    st.stop()
    
    st.markdown("""
    Esta seÃ§Ã£o documenta a arquitetura, estrutura de arquivos, tecnologias utilizadas
    e informaÃ§Ãµes sobre a equipe responsÃ¡vel pelo desenvolvimento do projeto.
    """)
    
    st.markdown("---")
    
    # MÃ©tricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("ðŸ’» Linhas de CÃ³digo", "20.000+", "Sistema completo")
    
    with col2:
        st.metric("ðŸ“Š PÃ¡ginas", "6", "Funcionalidades completas")
    
    with col3:
        st.metric("âš¡ OtimizaÃ§Ã£o", "70%+", "MemÃ³ria reduzida")
    
    with col4:
        st.metric("ðŸ“ Arquivos", "Parquet", "Formato otimizado")
        
    # EXPANDER 1: Estrutura de Arquivos
    with st.expander("ðŸ“ **Estrutura de Arquivos e OrganizaÃ§Ã£o do Projeto**", expanded=False):
        st.subheader("ðŸ“ Estrutura de Arquivos")
        
        st.markdown("""
        ### Estrutura do Projeto (visÃ£o de alto nÃ­vel)
        
        ```
        TC/
        â”œâ”€â”€ app.py                     # Portal / Router (menu via st.navigation)
        â”œâ”€â”€ pages/                     # PÃ¡ginas legadas (Waterfall/BE Simulador/ExtraÃ§Ã£o/DocumentaÃ§Ã£o)
        â”œâ”€â”€ tc_ext/                    # TC Ext (Linhas SecundÃ¡rias)
        â”œâ”€â”€ tc_principal/              # TC VeÃ­culos (TC Principal)
        â”œâ”€â”€ tc_core/                   # Shared (paths/portabilidade/perÃ­odos/schema/moedas/UI)
        â”œâ”€â”€ tc_copilot/                # IA (chat + relatÃ³rio PDF)
        â””â”€â”€ dados/                     # Dados organizados por mÃ³dulo
            â”œâ”€â”€ TC_Ext/                # dados/TC_Ext/{ANO}/, historico_consolidado/, Forecast/
            â””â”€â”€ TC_Principal/          # dados/TC_Principal/{ANO}/, historico_consolidado/, Forecast/
        ```
        
        **ObservaÃ§Ãµes:**
        - A estrutura de dados Ã© **por mÃ³dulo** (o histÃ³rico fica em `dados/TC_Ext/historico_consolidado/` e `dados/TC_Principal/historico_consolidado/`).
        - Caminhos canÃ´nicos (Dev â†” EXE) ficam em `tc_core/data/paths.py` + `tc_core/utils/portabilidade.py`.
        """)

        st.markdown("---")
        
        st.subheader("ðŸ“„ Arquivos Principais")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **app.py**
            - Portal/roteador do SCI (menu lateral via `st.navigation`)
            - Agrupa pÃ¡ginas de **TC VeÃ­culos**, **TC Ext**, **DocumentaÃ§Ã£o** e **TC Copilot**
            - NÃ£o contÃ©m a lÃ³gica de cÃ¡lculo â€” ela estÃ¡ nos mÃ³dulos `tc_ext/` e `tc_principal/`

            **tc_ext/pages/home_ext.py**
            - Home do TC Ext (Real/Budget/Flex/CPU)
            - Filtros + grÃ¡ficos + exportaÃ§Ã£o
            
            **pages/1 - Waterfall.py** (~4.000 linhas)
            - AnÃ¡lise waterfall entre perÃ­odos
            - CÃ¡lculo Flex MÃªs 1
            - GrÃ¡ficos waterfall interativos
            - Tabelas com hierarquia
            
            **pages/2 - Best Estimate - Simulador.py** (~4.300 linhas)
            - SimulaÃ§Ã£o interativa de Best Estimate
            - Ajuste de sensibilidade em tempo real
            - ConfiguraÃ§Ã£o de inflaÃ§Ã£o
            - GrÃ¡ficos de premissas
            """)
        
        with col2:
            st.markdown("""
            **tc_ext/pages/be_analise_ext.py**
            - Best Estimate (AnÃ¡lise) no TC Ext (substitui a anÃ¡lise legacy)
            - Mesma base visual e de cÃ¡lculo da Home (TC Ext)
            - LÃª os outputs do simulador em `dados/TC_Ext/Forecast/`
            - Regra de CPU aplicada de forma consistente (Total/Volume)
            
            **(removido) pages/4 - Waterfall_Analysis.py** (pÃ¡gina duplicada removida)
            - AnÃ¡lise waterfall entre perÃ­odos (legado)
            - CÃ¡lculo Flex MÃªs 1
            - GrÃ¡ficos waterfall interativos
            
            **pages/5 - ExtraÃ§Ã£o de Dados.py** (~600 linhas)
            - Interface para extraÃ§Ã£o e processamento de dados
            - Upload de arquivos
            - ValidaÃ§Ã£o de arquivos
            - ExecuÃ§Ã£o de notebooks de processamento
            
            **pages/6 - Documentacao.py** (~3.900 linhas)
            - DocumentaÃ§Ã£o completa do sistema
            - Regras e cÃ¡lculos
            - Arquitetura e estrutura
            - Guia de extraÃ§Ã£o de dados
            """)
    
    # Sub-expander: Estrutura da Pasta dados
    with st.expander("ðŸ“‚ **Estrutura e Funcionamento da Pasta `dados/`**", expanded=False):
        st.markdown("""
            ### ðŸ“‚ OrganizaÃ§Ã£o da Pasta `dados/`
            
            A pasta `dados/` Ã© o coraÃ§Ã£o do sistema, onde todos os arquivos processados sÃ£o armazenados.
            Ela Ã© organizada de forma hierÃ¡rquica para facilitar o gerenciamento e acesso aos dados.
            
            **Estrutura Completa (padronizada por mÃ³dulo):**
            ```
            dados/
            â”œâ”€â”€ TC_Ext/                         # ðŸ“Š TC Ext (Linhas SecundÃ¡rias)
            â”‚   â”œâ”€â”€ {ANO}/
            â”‚   â”‚   â”œâ”€â”€ df_final.parquet
            â”‚   â”‚   â”œâ”€â”€ df_vol.parquet
            â”‚   â”‚   â”œâ”€â”€ df_ke5z_group.parquet
            â”‚   â”‚   â”œâ”€â”€ Dados SAPIENS.xlsx
            â”‚   â”‚   â”œâ”€â”€ Reporting fluxo anexo.xlsx
            â”‚   â”‚   â””â”€â”€ BUD/
            â”‚   â”‚       â”œâ”€â”€ df_final_BUD.parquet
            â”‚   â”‚       â”œâ”€â”€ df_vol_BUD.parquet
            â”‚   â”‚       â””â”€â”€ df_ke5z_group_BUD.parquet
            â”‚   â”œâ”€â”€ historico_consolidado/
            â”‚   â”‚   â”œâ”€â”€ df_final_historico.parquet
            â”‚   â”‚   â”œâ”€â”€ df_ke5z_historico.parquet
            â”‚   â”‚   â”œâ”€â”€ df_vol_historico.parquet
            â”‚   â”‚   â””â”€â”€ BUD/
            â”‚   â”‚       â”œâ”€â”€ df_final_historico_BUD.parquet
            â”‚   â”‚       â”œâ”€â”€ df_ke5z_historico_BUD.parquet
            â”‚   â”‚       â””â”€â”€ df_vol_historico_BUD.parquet
            â”‚   â””â”€â”€ Forecast/                   # ðŸ”® Outputs do Best Estimate / Forecast (TC Ext)
            â”‚
            â””â”€â”€ TC_Principal/                   # ðŸš— TC VeÃ­culos (TC Principal)
                â”œâ”€â”€ {ANO}/
                â”‚   â”œâ”€â”€ df_principal.parquet
                â”‚   â”œâ”€â”€ df_tc_sapiens.parquet
                â”‚   â”œâ”€â”€ df_veiculos_custo_fp.parquet
                â”‚   â”œâ”€â”€ df_vol_veiculos_actual.parquet
                â”‚   â””â”€â”€ BUD/
                â”‚       â”œâ”€â”€ df_principal_BUD.parquet
                â”‚       â”œâ”€â”€ df_veiculos_custo_fp_BUD.parquet
                â”‚       â””â”€â”€ df_vol_veiculos_BUD.parquet
                â”œâ”€â”€ historico_consolidado/
                â””â”€â”€ Forecast/                   # ðŸ”® Outputs do Best Estimate (TC VeÃ­culos)
                    â”œâ”€â”€ forecast_completo.parquet
                    â””â”€â”€ premissas.json
            ```
            """)
            
        st.markdown("---")
            
        st.markdown("""
            ### ðŸ”„ Como as Pastas SÃ£o Criadas e Atualizadas
            
            **1. CriaÃ§Ã£o Inicial da Estrutura:**
            
            Quando o sistema Ã© executado pela primeira vez ou quando novos dados sÃ£o processados,
            o sistema verifica e cria automaticamente as pastas necessÃ¡rias:
            
            ```python
            # Caminhos canÃ´nicos (dev â†” EXE) â€” tc_core/data/paths.py
            from tc_core.data.paths import PASTA_TC_EXT, PASTA_TC_PRINCIPAL

            # TC Ext
            pasta_ano_tc_ext = f"{PASTA_TC_EXT}/{ANO_ATUAL}"            # dados/TC_Ext/{ANO}
            pasta_bud_tc_ext = f"{pasta_ano_tc_ext}/BUD"               # dados/TC_Ext/{ANO}/BUD
            pasta_hist_tc_ext = f"{PASTA_TC_EXT}/historico_consolidado" # dados/TC_Ext/historico_consolidado

            # TC VeÃ­culos
            pasta_ano_tc_principal = f"{PASTA_TC_PRINCIPAL}/{ANO_ATUAL}" # dados/TC_Principal/{ANO}
            pasta_bud_tc_principal = f"{pasta_ano_tc_principal}/BUD"     # dados/TC_Principal/{ANO}/BUD
            ```
            
            **2. Processo de AtualizaÃ§Ã£o:**
            
            **a) Processamento de Dados do Ano:**
            - Os arquivos Excel (`Dados SAPIENS.xlsx`, `Reporting fluxo anexo.xlsx`) do **TC Ext** ficam em `dados/TC_Ext/{ANO}/`
            - O notebook `tc_ext/notebooks/dados.ipynb` processa esses arquivos e gera os arquivos Parquet
            - Os arquivos Parquet sÃ£o salvos na mesma pasta do ano (`dados/TC_Ext/{ANO}/`)
            - **Simultaneamente**, os dados sÃ£o consolidados no histÃ³rico
            
            **b) ConsolidaÃ§Ã£o no HistÃ³rico:**
            - ApÃ³s processar os dados do ano, o sistema **concatena** os novos dados com o histÃ³rico existente
            - Os arquivos em `historico_consolidado/` sÃ£o **atualizados** (nÃ£o substituÃ­dos)
            - Isso permite que o sistema tenha acesso a **todos os dados histÃ³ricos** em um Ãºnico lugar
            
            **c) Processamento de Budget:**
            - Similar ao processo de dados do ano, mas os arquivos sÃ£o processados pelo `tc_ext/notebooks/dados_BUD.ipynb`
            - Os **outputs** de Budget do **TC Ext** sÃ£o salvos em `dados/TC_Ext/{ANO}/BUD/`
            - O histÃ³rico de Budget do **TC Ext** Ã© consolidado em `dados/TC_Ext/historico_consolidado/BUD/`
            
            **d) Processamento de Forecast:**
            - Forecast do **TC Ext**: outputs em `dados/TC_Ext/Forecast/`
            - Forecast do **TC VeÃ­culos**: outputs em `dados/TC_Principal/Forecast/`
            """)
        
        st.markdown("---")
        
        st.markdown("""
        ### ðŸ”— Como as Pastas Funcionam Entre Si
            
            **1. RelaÃ§Ã£o entre Pastas por Ano e HistÃ³rico:**
            
            ```
            dados/TC_Ext/2026/df_final.parquet  â”€â”€â”
                                                  â”œâ”€â”€> Concatena â”€â”€> dados/TC_Ext/historico_consolidado/df_final_historico.parquet
            dados/TC_Ext/2025/df_final.parquet  â”€â”€â”˜
            ```
            
            - **Dados do Ano:** ContÃªm apenas os dados do ano especÃ­fico (Ãºtil para filtros rÃ¡pidos)
            - **HistÃ³rico Consolidado:** ContÃ©m **TODOS** os anos concatenados (usado pelo sistema principal)
            - O sistema **prioriza** o histÃ³rico consolidado para anÃ¡lises que precisam de mÃºltiplos anos
            
            **2. Fluxo de Dados:**
            
            ```
            Arquivos Excel (entrada)
                â”‚
                â”œâ”€â”€> Processamento (tc_ext/notebooks/dados.ipynb) â€” TC Ext
                â”‚       â”‚
                â”‚       â”œâ”€â”€> Salva em dados/TC_Ext/{ANO}/ (dados do ano)
                â”‚       â”‚
                â”‚       â””â”€â”€> Concatena em dados/TC_Ext/historico_consolidado/ (histÃ³rico completo)
                â”‚
                â””â”€â”€> Sistema Streamlit lÃª de dados/TC_Ext/historico_consolidado/ (fonte principal do TC Ext)
            ```
            
            **3. SeparaÃ§Ã£o de Budget:**
            
            - **TC Ext (Real):** `dados/TC_Ext/{ANO}/` e `dados/TC_Ext/historico_consolidado/`
            - **TC Ext (Budget):** `dados/TC_Ext/{ANO}/BUD/` e `dados/TC_Ext/historico_consolidado/BUD/`
            - Esta separaÃ§Ã£o evita misturar outputs de Budget com Real
            
            **4. Forecast como Dados Derivados:**
            
            - As pastas `Forecast/` contÃªm dados **processados e calculados** pelo sistema
            - NÃ£o sÃ£o dados de entrada, mas sim **resultados** de cÃ¡lculos de forecast
            - SÃ£o gerados dinamicamente quando o usuÃ¡rio executa o Forecast
            """)
        
        st.markdown("---")
        
        st.markdown("""
        ### âš™ï¸ Regras de CriaÃ§Ã£o e AtualizaÃ§Ã£o
            
            **Regra 1: CriaÃ§Ã£o AutomÃ¡tica**
            - Todas as pastas sÃ£o criadas automaticamente quando necessÃ¡rio
            - O parÃ¢metro `exist_ok=True` garante que nÃ£o hÃ¡ erro se a pasta jÃ¡ existir
            - NÃ£o Ã© necessÃ¡rio criar manualmente nenhuma pasta
            
            **Regra 2: ConsolidaÃ§Ã£o Incremental**
            - O histÃ³rico Ã© **atualizado** (nÃ£o substituÃ­do) a cada processamento
            - Novos dados sÃ£o **adicionados** ao histÃ³rico existente
            - Isso mantÃ©m a integridade dos dados histÃ³ricos
            
            **Regra 3: SeparaÃ§Ã£o por Tipo**
            - Dados Reais e Budget sÃ£o mantidos **separados** em pastas diferentes
            - Isso evita confusÃ£o e permite comparaÃ§Ãµes precisas
            - O sistema sabe qual pasta usar baseado no modo de comparaÃ§Ã£o selecionado
            
            **Regra 4: Formato Parquet**
            - Todos os arquivos processados sÃ£o salvos em formato **Parquet**
            - Parquet oferece compressÃ£o e leitura rÃ¡pida
            - Formato otimizado para grandes volumes de dados
            """)
    
    # EXPANDER 2: Tecnologias
    with st.expander("ðŸ’» **Tecnologias e Bibliotecas**", expanded=False):
        st.subheader("ðŸ’» Tecnologias e Bibliotecas")
        
        st.markdown(f"""
        ### Stack TecnolÃ³gico
        
        **Framework Principal:**
        - **Streamlit** {st.__version__} - Framework web para aplicaÃ§Ãµes de dados
        
        **Linguagem:**
        - **Python** 3.8+ - Linguagem de programaÃ§Ã£o
        
        **Processamento de Dados:**
        - **Pandas** 2.0.0+ - ManipulaÃ§Ã£o e anÃ¡lise de dados
        - **NumPy** 1.24.0+ - OperaÃ§Ãµes numÃ©ricas
        
        **VisualizaÃ§Ãµes:**
        - **Altair** 5.0.0+ - GrÃ¡ficos interativos
        - **Plotly** - GrÃ¡ficos waterfall avanÃ§ados
        
        **Formato de Dados:**
        - **PyArrow** 12.0.0+ - Suporte a Parquet
        - **Parquet** - Formato de dados otimizado
        
        **ExportaÃ§Ã£o:**
        - **OpenPyXL** 3.1.0+ - GeraÃ§Ã£o de arquivos Excel
        """)
        
        st.markdown("---")
        
        st.subheader("ðŸ”§ DependÃªncias Principais")
        
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
        
        st.subheader("âš¡ OtimizaÃ§Ãµes Implementadas")
        
        st.markdown("""
        **GestÃ£o de MemÃ³ria:**
        - Cache inteligente com TTL configurÃ¡vel
        - OtimizaÃ§Ã£o de tipos: Category para strings repetidas
        - Downcast: Float64 -> Float32, Int64 -> Int32
        - ReduÃ§Ã£o de cÃ³pias: Apenas quando necessÃ¡rio
        
        **OperaÃ§Ãµes Vetorizadas:**
        - SubstituiÃ§Ã£o de `iterrows()` por merge e `np.where()`
        - SubstituiÃ§Ã£o de `apply()` por operaÃ§Ãµes vetorizadas
        - Filtros booleanos ao invÃ©s de loops
        - Agrupamento otimizado com `agg()` direto
        
        **CÃ¡lculos Otimizados:**
        - CPU calculado apÃ³s agrupamento (nunca antes)
        - Flex Bud com merge ao invÃ©s de loops
        - Volume sincronizado entre tabelas e grÃ¡ficos
        - Cache de filtros para opÃ§Ãµes repetidas
        """)
    
    # EXPANDER 3: Desafios e SoluÃ§Ãµes
    with st.expander("âš ï¸ **Desafios Principais & SoluÃ§Ãµes Implementadas**", expanded=False):
        st.markdown("""
        ### ðŸ“Š Desafios Identificados
        
        - **ðŸ“ Dados grandes:** MilhÃµes de registros causando lentidÃ£o
        - **ðŸ’¾ Uso de memÃ³ria:** Excedia limites de processamento
        - **Instabilidade:** Sistema lento com muitos filtros
        - **ðŸŒ CÃ¡lculos complexos:** Flex Bud e Forecast demorados
        - **ðŸ”„ SincronizaÃ§Ã£o:** Dados de tabela vs grÃ¡ficos diferentes
        - **ðŸ“Š VisualizaÃ§Ãµes:** GrÃ¡ficos sem gradientes e pouco informativos
        """)
        
        st.markdown("---")
        
        st.markdown("""
        ### âœ… SoluÃ§Ãµes Implementadas
        
        - **ðŸ“Š OtimizaÃ§Ã£o de dados:** Parquet com tipos categÃ³ricos
        - **âš¡ Cache estratÃ©gico:** TTL configurÃ¡vel por tipo de dado
        - **ðŸ”„ OperaÃ§Ãµes vetorizadas:** SubstituiÃ§Ã£o de iterrows() e apply()
        - **ðŸ“ˆ CÃ¡lculos otimizados:** Flex Bud e CPU apÃ³s agrupamento
        - **ðŸŽ¯ SincronizaÃ§Ã£o:** Mesma fonte de dados para tabelas e grÃ¡ficos
        - **ðŸŽ¨ VisualizaÃ§Ãµes melhoradas:** Gradientes, delta charts, barras HTML
        """)
        
        st.info("ðŸŽ† **Resultado Final:** Sistema 100% estÃ¡vel com performance otimizada e visualizaÃ§Ãµes profissionais!")
    
    # EXPANDER 4: EstatÃ­sticas do Sistema
    with st.expander("ðŸ“Š **EstatÃ­sticas e MÃ©tricas do Sistema**", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### ðŸ’¾ Dados e Performance
            
            **ðŸ“ Arquivos Principais:**
            - `df_final_historico.parquet` (dados histÃ³ricos)
            - `df_vol_historico.parquet` (volumes)
            - `df_final_historico_BUD.parquet` (budget)
            
            **âš¡ OtimizaÃ§Ãµes:**
            - Tipos categÃ³ricos para strings
            - Downcast de numÃ©ricos
            - CompressÃ£o Parquet
            - Cache com TTL
            """)
        
        with col2:
            st.markdown("""
            ### ðŸ“Š PÃ¡ginas do Sistema
            
            **ðŸ“„ PÃ¡ginas DisponÃ­veis:**
            - `app.py` - Portal / Router (menu via st.navigation)
            - `1 - Waterfall.py` - AnÃ¡lise waterfall (~4.000 linhas)
            - `2 - Best Estimate - Simulador.py` - SimulaÃ§Ã£o (~4.300 linhas)
            - `tc_ext/pages/be_analise_ext.py` - Best Estimate (AnÃ¡lise) (base Home)
            - `4 - Waterfall_Analysis.py` - (removido) pÃ¡gina duplicada
            - `5 - ExtraÃ§Ã£o de Dados.py` - ExtraÃ§Ã£o e processamento (~600 linhas)
            - `6 - Documentacao.py` - DocumentaÃ§Ã£o (~3.900 linhas)
            
            **ðŸ“Š Total:** ~33.000+ linhas de cÃ³digo
            """)
        
        with col3:
            st.markdown(f"""
            ### ðŸ”§ Tecnologias
            
            **Stack Principal:**
            - Streamlit {st.__version__}
            - Pandas {pd.__version__}
            - NumPy {np.__version__}
            - Altair (versÃ£o instalada)
            - Plotly (versÃ£o instalada)
            - OpenPyXL (versÃ£o instalada)
            """)

# ==========================================
# TC VEÃCULOS: ESPECIFICAÃ‡ÃƒO TÃ‰CNICA
# ==========================================
elif indice_selecionado == "ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica" and modulo_doc.startswith("ðŸ“Œ Ambos"):
    st.header("ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica â€” TC Ext + TC VeÃ­culos")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("ðŸ“Š TC Estendido")
        caminho_doc = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")
        if not os.path.exists(caminho_doc):
            st.error(f"Arquivo nÃ£o encontrado: {caminho_doc}")
        else:
            try:
                mtime_doc = os.path.getmtime(caminho_doc)
                st.caption(
                    f"Fonte: {caminho_doc} | Atualizado em: {_formatar_mtime(mtime_doc)}"
                )
                conteudo = _ler_arquivo_texto_cacheado(caminho_doc, mtime_doc)
                st.markdown(conteudo)
            except Exception as e:
                st.error(f"Erro ao carregar especificaÃ§Ã£o: {e}")

    with col2:
        st.subheader("ðŸš— TC VeÃ­culos")
        caminho_doc_tc = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")
        if not os.path.exists(caminho_doc_tc):
            st.error(f"Arquivo nÃ£o encontrado: {caminho_doc_tc}")
        else:
            try:
                mtime_tc = os.path.getmtime(caminho_doc_tc)
                st.caption(
                    f"Fonte: {caminho_doc_tc} | Atualizado em: {_formatar_mtime(mtime_tc)}"
                )
                conteudo_tc = _ler_arquivo_texto_cacheado(caminho_doc_tc, mtime_tc)
                st.markdown(conteudo_tc)
            except Exception as e:
                st.error(f"Erro ao carregar especificaÃ§Ã£o TC VeÃ­culos: {e}")

    st.stop()

elif indice_selecionado == "ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica" and modulo_doc == "ðŸš— TC VeÃ­culos":
    st.header("ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica â€” TC VeÃ­culos")

    st.markdown(
        """
        EspecificaÃ§Ã£o tÃ©cnica completa do mÃ³dulo **TC VeÃ­culos** em formato Markdown.
        Arquivo fonte: `DOCUMENTACAO_TC_PRINCIPAL.md`
        """
    )

    _caminho_doc_tc = os.path.join(get_base_path(), "DOCUMENTACAO_TC_PRINCIPAL.md")

    if not os.path.exists(_caminho_doc_tc):
        st.error(f"Arquivo nÃ£o encontrado: {_caminho_doc_tc}")
    else:
        try:
            _mtime_tc = os.path.getmtime(_caminho_doc_tc)
            st.caption(
                f"Fonte: {_caminho_doc_tc} | Atualizado em: {_formatar_mtime(_mtime_tc)}"
            )

            _conteudo_tc = _ler_arquivo_texto_cacheado(_caminho_doc_tc, _mtime_tc)

            # Sem botÃµes de download: a especificaÃ§Ã£o deve estar toda escrita na pÃ¡gina.
            st.markdown("---")
            st.markdown(_conteudo_tc)
        except Exception as e:
            st.error(f"Erro ao carregar especificaÃ§Ã£o TC VeÃ­culos: {e}")

# ==========================================
# SEÃ‡ÃƒO 3: ESPECIFICAÃ‡ÃƒO TÃ‰CNICA (REESCRITA)
# ==========================================
elif indice_selecionado == "ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica":
    st.header("ðŸ§¾ EspecificaÃ§Ã£o TÃ©cnica â€” TC Estendido")

    st.markdown(
        """
        Esta seÃ§Ã£o consolida uma **especificaÃ§Ã£o tÃ©cnica completa** em formato Markdown.
        O objetivo Ã© permitir que vocÃª reescreva o projeto com IA preservando:
        - funcionalidades
        - regras de cÃ¡lculo (CPU/Flex Bud)
        - fontes de dados e contratos (schemas)
        - comportamento de filtros e grÃ¡ficos
        """
    )

    caminho_doc = os.path.join(get_base_path(), "DOCUMENTACAO_SISTEMA_TC.md")

    if not os.path.exists(caminho_doc):
        st.error(f"Arquivo nÃ£o encontrado: {caminho_doc}")
    else:
        try:
            mtime_doc = os.path.getmtime(caminho_doc)
            st.caption(
                f"Fonte: {caminho_doc} | Atualizado em: {_formatar_mtime(mtime_doc)}"
            )

            conteudo = _ler_arquivo_texto_cacheado(caminho_doc, mtime_doc)

            # Sem botÃµes de download: a especificaÃ§Ã£o deve estar toda escrita na pÃ¡gina.
            st.markdown("---")
            st.markdown(conteudo)
        except Exception as e:
            st.error(f"Erro ao carregar especificaÃ§Ã£o: {e}")

# ==========================================
# SEÃ‡ÃƒO 4: GUIA DE EXTRAÃ‡ÃƒO DE DADOS
# ==========================================
elif indice_selecionado == "ðŸ“¥ Guia de ExtraÃ§Ã£o de Dados":
    st.header("ðŸ“¥ Guia de ExtraÃ§Ã£o de Dados â€” TC Estendido")
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
    <h2 style="color: white; margin: 0;">ðŸ“š DocumentaÃ§Ã£o Completa para IA</h2>
    <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Todos os Relacionamentos, Processos e Estruturas de Dados
    </p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("ðŸš— **TC VeÃ­culos â€” Pipeline completo (Real e Budget)**", expanded=False):
        st.markdown("""
        ### ðŸ”„ Fluxo de Processamento (TC VeÃ­culos)

        ```
        Arquivos Excel (Entrada)
            â”‚
            â”œâ”€â”€> processamento_dados_veiculos_BUD.py (Budget)
            â”‚       â”œâ”€â”€> LÃª Budget + D&A dedicado + volumes/tempos
            â”‚       â”œâ”€â”€> Normaliza colunas e perÃ­odos
            â”‚       â”œâ”€â”€> Calcula composiÃ§Ã£o (FA/FP) e rateios por veÃ­culo
            â”‚       â”œâ”€â”€> Grava df_principal_BUD.parquet
            â”‚       â”œâ”€â”€> Grava df_veiculos_custo_fp_BUD.parquet
            â”‚       â””â”€â”€> Grava df_veiculos_cpu_BUD.parquet
            â”‚
            â””â”€â”€> processamento_dados_veiculos.py (Real)
                    â”œâ”€â”€> LÃª Sapiens + Redis + volumes
                    â”œâ”€â”€> Normaliza e processa
                    â”œâ”€â”€> Grava df_principal.parquet
                    â”œâ”€â”€> Grava df_tc_sapiens.parquet (detalhado)
                    â””â”€â”€> Grava df_veiculos_custo_fp.parquet / df_veiculos_cpu.parquet
        ```

        **PÃ¡gina Streamlit que executa o fluxo:** `tc_principal/pages/extracao_dados_tc.py`

        ### âœ… Arquivo de entrada (fonte Ãºnica)

        - `Reporting veÃ­culos.xlsx` em `dados/TC_Principal/{ano}/`
        - A pÃ¡gina `extracao_dados_tc.py` permite **upload** com proteÃ§Ã£o contra sobrescrita (checkbox de confirmaÃ§Ã£o)

        ### ðŸ§¾ Abas obrigatÃ³rias â€” Budget (no Excel)

        - `massa primÃ¡ria - BDG`
        - `massa - REDIS`
        - `Volume e EST PdR - BDG`
        - `Volume BDG`
        - `Volume Actual`
        - `EST veÃ­culos - BDG`
        - `massa - D&A dedicado`

        ### ðŸ§¾ Abas obrigatÃ³rias â€” Real (no Excel)

        - `Sapiens`
        - `Volume e EST PdR - Actual`
        - `Volume Actual`
        - `EST veÃ­culos - Actual`

        ### ðŸ”Ž PrÃ©-validaÃ§Ã£o (o que o app checa antes de processar)

        - Se as abas obrigatÃ³rias existem
        - Budget: colunas mÃ­nimas em `massa primÃ¡ria - BDG` (ex.: `Oficina`, `Account`) e `massa - REDIS` (ex.: `Oficina`)
        - Budget: detecÃ§Ã£o de meses em `Volume BDG` (tentando mÃºltiplos headers)
        - Real: em `Sapiens`, valida colunas mÃ­nimas (ex.: `Oficina`, `Account`, `Valor`)
        - Aviso operacional: para o fluxo completo, o Real depende do Budget ter gerado `df_dea_dedicado_BUD.parquet`
        - Rateios manuais (QY/GS/SM): persistidos em `rateios_manuais.json` (usados no cÃ¡lculo da taxa PdR)

        ### ðŸ§± ConsolidaÃ§Ã£o histÃ³rica (multi-ano)

        A pÃ¡gina tambÃ©m consolida parquets multi-ano em `dados/TC_Principal/historico_consolidado/`.

        ### ðŸ“‚ Scripts e FunÃ§Ãµes

        | Arquivo | FunÃ§Ã£o |
        |---------|--------|
        | `tc_principal/pages/extracao_dados_tc.py` | Orquestra execuÃ§Ã£o e gravaÃ§Ã£o dos parquets (Real/Budget) |
        | `processamento_dados_veiculos_BUD.py` | Processa Budget + gera parquets BUD (principal + por veÃ­culo + CPU) |
        | `processamento_dados_veiculos.py` | Processa Real (Sapiens/Redis) + gera parquets Real (principal + por veÃ­culo + CPU) |

        ### ðŸ—ƒï¸ Principais parquets gerados

        **Budget** (`dados/TC_Principal/{ano}/BUD/`):
        - `df_principal_BUD.parquet`
        - `df_vol_veiculos_BUD.parquet` / `df_vol_veiculos_actual.parquet`
        - `df_tempo_veiculos_BUD.parquet`
        - `df_dea_dedicado_BUD.parquet`
        - `df_veiculos_percentual_rateio_BUD.parquet` / `df_veiculos_custo_rateado_BUD.parquet`
        - `df_veiculos_custo_fp_BUD.parquet` / `df_veiculos_cpu_BUD.parquet`

        **Real** (`dados/TC_Principal/{ano}/`):
        - `df_principal.parquet`
        - `df_tc_sapiens.parquet` (detalhado)
        - `df_vol_veiculos.parquet` / `df_tempo_veiculos.parquet` / `df_dea_dedicado.parquet`
        - `df_veiculos_percentual_rateio.parquet` / `df_veiculos_custo_rateado.parquet`
        - `df_veiculos_custo_fp.parquet` / `df_veiculos_cpu.parquet`
        - `df_comparativo_real_budget.parquet`

        ### ðŸ“ Pastas (entrada e saÃ­da)
        - Entrada: `dados/TC_Principal/{ano}/` (Excel/insumos)
        - SaÃ­da Real: `dados/TC_Principal/{ano}/` (parquets Real)
        - SaÃ­da Budget: `dados/TC_Principal/{ano}/BUD/` (parquets BUD)

        ### ðŸ“Š Dados de Volume (usos)
        Os volumes sÃ£o usados para:
        - CPU
        - Flex Budget (proporÃ§Ã£o Real/BUD)
        - GrÃ¡ficos comparativos (BUD vs Real)
        """)
    
    # Ãndice interno
    st.markdown("## ðŸ“‹ Ãndice do Guia")
    st.markdown("""
    ### ðŸ“– CapÃ­tulo 1: Estrutura e Processamento dos Notebooks
    1. [VisÃ£o Geral](#visao-geral)
    2. [Notebook tc_ext/notebooks/dados.ipynb - Dados REAIS](#dados-reais)
    3. [Notebook tc_ext/notebooks/dados_BUD.ipynb - Dados BUDGET](#dados-budget)
    4. [Estrutura de Arquivos de Entrada](#estrutura-entrada)
    5. [Relacionamentos e Merges](#relacionamentos)
    6. [Colunas e Estrutura Final](#colunas-finais)
    7. [ConsolidaÃ§Ã£o do HistÃ³rico](#consolidacao)
    8. [Arquivos de SaÃ­da](#arquivos-saida)
    9. [Fluxo Completo](#fluxo-completo)
    10. [Tratamento de Erros](#tratamento-erros)
    11. [Checklist para ManutenÃ§Ã£o](#checklist)
    
    ### ðŸ”„ CapÃ­tulo 2: Funcionamento da AtualizaÃ§Ã£o e ExtraÃ§Ã£o
    1. [VisÃ£o Geral do Processo de AtualizaÃ§Ã£o](#visao-atualizacao)
    2. [Ordem CronolÃ³gica dos Eventos](#ordem-cronologica)
    3. [Sistema de Busca de Arquivos](#busca-arquivos)
    4. [CriaÃ§Ã£o de Pastas e Estrutura](#criacao-pastas)
    5. [Sistema de Upload de Arquivos](#sistema-upload)
    6. [Processamento e ExecuÃ§Ã£o](#processamento-execucao)
    7. [CenÃ¡rios de Uso](#cenarios-uso)
    """)
    
    st.markdown("---")
    
    # ==========================================
    # CAPÃTULO 1: ESTRUTURA E PROCESSAMENTO DOS NOTEBOOKS
    # ==========================================
    
    with st.expander("ðŸ“– **CapÃ­tulo 1: Estrutura e Processamento dos Notebooks**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">ðŸ“– CapÃ­tulo 1: Estrutura e Processamento dos Notebooks</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                DocumentaÃ§Ã£o Completa dos Notebooks de ExtraÃ§Ã£o - Estrutura, Processamento e Relacionamentos
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # SeÃ§Ã£o 1: VisÃ£o Geral
        st.markdown("## ðŸŽ¯ VISÃƒO GERAL {#visao-geral}")
        
        st.markdown("### Objetivo dos Notebooks")
        st.markdown("""
        Os notebooks `tc_ext/notebooks/dados.ipynb` e `tc_ext/notebooks/dados_BUD.ipynb` sÃ£o responsÃ¡veis por:
        - **Carregar** dados de mÃºltiplas fontes (Excel: SAPIENS, Reporting fluxo anexo)
        - **Processar** e **normalizar** dados de diferentes formatos e guias
        - **Unificar** informaÃ§Ãµes atravÃ©s de merges por chaves comuns
        - **Calcular** rateios por veÃ­culo (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)
        - **Gerar** arquivos Parquet e Excel otimizados para uso no dashboard
        - **Consolidar** dados histÃ³ricos para anÃ¡lises multi-anos
        """)
        
        st.markdown("### DiferenÃ§a entre tc_ext/notebooks/dados.ipynb e tc_ext/notebooks/dados_BUD.ipynb")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **ðŸ“Š tc_ext/notebooks/dados.ipynb - Dados REAIS**
        - Processa dados de custos **reais** (executados)
        - LÃª guia **"Sapiens"** do Reporting fluxo anexo.xlsx
        - LÃª guia **"Rateio"** para rateio por veÃ­culo
        - LÃª guia **"Volume"** para volumes
        - Salva em: `dados/TC_Ext/{ANO}/`
        - HistÃ³rico: `dados/TC_Ext/historico_consolidado/`
        """)
        
        with col2:
            st.markdown("""
            **ðŸ“ˆ tc_ext/notebooks/dados_BUD.ipynb - Dados BUDGET**
        - Processa dados de **orÃ§amento/planejamento** (Budget)
        - LÃª guia **"Voz de custo BDG"** do Reporting fluxo anexo.xlsx
        - LÃª guia **"Rateio BDG"** para rateio por veÃ­culo
        - LÃª guia **"Volume BDG"** para volumes
        - Salva em: `dados/TC_Ext/{ANO}/BUD/`
        - HistÃ³rico: `dados/TC_Ext/historico_consolidado/BUD/`
        """)
        
        st.markdown("### Fluxo Principal")
        st.code("""
        Arquivos Excel (Entrada)
            â”‚
            â”œâ”€â”€> tc_ext/notebooks/dados.ipynb (REAL)
            â”‚       â”œâ”€â”€> Processamento
            â”‚       â”œâ”€â”€> Merges (Account, NÂº conta, Centro cst, Oficina+PerÃ­odo)
            â”‚       â”œâ”€â”€> CÃ¡lculo Rateio por VeÃ­culo
            â”‚       â”œâ”€â”€> Merge com Volume
            â”‚       â””â”€â”€> Salvar Parquet + Consolidar HistÃ³rico
            â”‚
            â””â”€â”€> tc_ext/notebooks/dados_BUD.ipynb (BUDGET)
                    â”œâ”€â”€> Processamento (mesma lÃ³gica)
                    â”œâ”€â”€> Merges (mesmas chaves)
                    â”œâ”€â”€> CÃ¡lculo Rateio por VeÃ­culo
                    â”œâ”€â”€> Merge com Volume
                    â””â”€â”€> Salvar Parquet (BUD) + Consolidar HistÃ³rico (BUD)
        """, language="text")
        
        st.markdown("---")
        
        # SeÃ§Ã£o 2: tc_ext/notebooks/dados.ipynb - Dados REAIS
        st.markdown("## ðŸ“Š NOTEBOOK tc_ext/notebooks/dados.ipynb - DADOS REAIS {#dados-reais}")
        
        st.markdown("### Estrutura do Processamento")
        
        with st.expander("ðŸ”§ **CÃ©lula 0: ConfiguraÃ§Ã£o Inicial**", expanded=False):
            st.markdown("""
            **Objetivo**: Configurar ano, pastas e caminhos
            
            **Processo**:
            1. Solicita ano para processar (padrÃ£o: ano atual)
            2. Cria estrutura de pastas:
                    - `dados/TC_Ext/{ANO_ATUAL}/` - Dados do ano especÃ­fico (TC Ext)
                    - `dados/TC_Ext/historico_consolidado/` - HistÃ³rico consolidado (TC Ext)
            3. Verifica arquivos de entrada:
               - `Dados SAPIENS.xlsx`
               - `Reporting fluxo anexo.xlsx`
            4. Define caminhos de entrada e saÃ­da
            
            **VariÃ¡veis Criadas**:
            - `ANO_ATUAL`: Ano selecionado para processamento
            - `PASTA_ANO`: `dados/TC_Ext/{ANO_ATUAL}/`
            - `PASTA_HISTORICO`: `dados/TC_Ext/historico_consolidado/`
            - `CAMINHO_SAPIENS`: Caminho para Dados SAPIENS.xlsx
            - `CAMINHO_RATEIO`: Caminho para Reporting fluxo anexo.xlsx
            - `CAMINHO_DF_FINAL`: `dados/TC_Ext/{ANO}/df_final.parquet`
            - `CAMINHO_DF_VOL`: `dados/TC_Ext/{ANO}/df_vol.parquet`
            - `CAMINHO_DF_KE5Z_GROUP`: `dados/TC_Ext/{ANO}/df_ke5z_group.parquet`
            """)
        
        with st.expander("ðŸ“¥ **CÃ©lula 1: Leitura dos Dados SAPIENS (KE5Z)**", expanded=False):
            st.markdown("""
            **Arquivo**: `Reporting fluxo anexo.xlsx`
            **Guia**: `"Sapiens"`
            **CabeÃ§alho**: Linha 1 (`header=1`)
            **Colunas**: A atÃ© T (20 colunas, `usecols=range(20)`)
            
            **Colunas Lidas**:
            - `Mes`, `PerÃ­odo`, `NÂºconta`, `Centrocst`, `NÂºdoc.ref.`, `Dt.lÃ§to.`
            - `Valor`, `QTD`, `Type 05`, `Type 06`, `Account` (Type 07)
            - `USI`, `Oficina`, `Doc.compra`, `Texto breve`
            - `Fornecedor`, `Material`, `UsuÃ¡rio`, `Fornec.`, `Tipo`
            
            **DataFrame Criado**: `df_KE5Z`
            
            **ValidaÃ§Ã£o**: Soma da coluna `Valor` para verificar leitura
            """)
        
        with st.expander("ðŸ”— **CÃ©lula 2: Merge com Base Conso (Custo)**", expanded=False):
            st.markdown("""
            **Arquivo**: `Dados SAPIENS.xlsx`
            **Guia**: `"Base conso"`
            
            **Processo**:
            1. LÃª guia "Base conso"
            2. Renomeia `Type 04` â†’ `Custo` (se existir)
            3. MantÃ©m apenas colunas: `Custo`, `Type 07`
            4. Renomeia `Type 07` â†’ `Account`
            5. Faz merge com `df_KE5Z` usando `Account` como chave
            
            **Chave de Merge**: `Account` (Type 07)
            **Tipo**: `left` (mantÃ©m todos os registros de KE5Z)
            
            **Resultado**: Adiciona coluna `Custo` ao `df_KE5Z`
            - Valores possÃ­veis: `"VariÃ¡vel"` ou `"Fixo"`
            """)
        
        with st.expander("ðŸ“Š **CÃ©lula 3: Processamento de Rateio**", expanded=False):
            st.markdown("""
            **Arquivo**: `Reporting fluxo anexo.xlsx`
            **Guia**: `"Rateio"`
            
            **Processo**:
            1. LÃª guia sem header (`header=None`)
            2. Remove primeira linha (linha de referÃªncia)
            3. Usa segunda linha como cabeÃ§alho (meses)
            4. Remove linha usada como cabeÃ§alho
            5. Identifica colunas de meses (janeiro a dezembro)
            6. Usa `melt()` para transformar colunas de meses em linhas
            7. Cria colunas: `PerÃ­odo` (mÃªs) e `Rateio` (valor)
            8. Normaliza `PerÃ­odo` para capitalizado (Janeiro, Fevereiro, etc.)
            9. Filtra: Remove `Oficina == 'VeÃ­culos'` e linhas com `Oficina` NaN
            
            **Colunas de IdentificaÃ§Ã£o (id_vars)**:
            - `Oficina`, `VeÃ­culo` (e outras colunas nÃ£o-mÃªs)
            
            **Colunas Transformadas (value_vars)**:
            - Meses: Janeiro, Fevereiro, MarÃ§o, ..., Dezembro
            
            **DataFrame Criado**: `df` (com colunas: `Oficina`, `VeÃ­culo`, `PerÃ­odo`, `Rateio`)
            """)
        
        with st.expander("ðŸ”„ **CÃ©lula 4: Merge KE5Z â†” Rateio e CÃ¡lculo por VeÃ­culo**", expanded=False):
            st.markdown("""
            **Chave de Merge**: `['Oficina', 'PerÃ­odo']` (COMPOSTA)
            **Tipo**: `left` (mantÃ©m todos os registros de KE5Z)
            
            **Processo**:
            1. Merge `df_KE5Z` com `df` (rateio) usando `['Oficina', 'PerÃ­odo']`
            2. Pivot: Transforma `VeÃ­culo` em colunas de `Rateio`
               - Index: `['Oficina', 'PerÃ­odo']`
               - Columns: `VeÃ­culo` (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)
               - Values: `Rateio` (percentuais)
               - Aggfunc: `mean` (mÃ©dia para agregar duplicatas)
            3. Renomeia colunas de veÃ­culos: adiciona `%` (ex: `CC21%`, `CC22%`)
            4. Merge reverso: `df_KE5Z` com `df_pivot` usando `['Oficina', 'PerÃ­odo']`
            5. Calcula colunas de valores por veÃ­culo:
               - `CC21 = CC21% * Valor`
               - `CC22 = CC22% * Valor`
               - `CC24 = CC24% * Valor`
               - `CC24 5L = CC24 5L% * Valor`
               - `CC24 7L = CC24 7L% * Valor`
               - `J516 = J516% * Valor`
            6. Calcula `Soma_Percentuais = CC21% + CC22% + ... + J516%`
            7. Remove colunas de percentual (`CC21%`, `CC22%`, etc.)
            
            **Resultado**: `df_final` com colunas de valores por veÃ­culo calculadas
            """)
        
        with st.expander("ðŸ“ˆ **CÃ©lula 5: Processamento de Volume**", expanded=False):
            st.markdown("""
            **Arquivo**: `Reporting fluxo anexo.xlsx`
            **Guia**: `"Volume"`
            **CabeÃ§alho**: Linha 51 (`header=50`, 0-indexed)
            
            **Processo**:
            1. LÃª guia "Volume" com cabeÃ§alho na linha 51
            2. Identifica colunas de meses (janeiro a dezembro)
            3. Usa `melt()` para transformar colunas de meses em linhas
            4. Cria colunas: `PerÃ­odo` (mÃªs) e `Volume` (valor)
            5. Normaliza `PerÃ­odo` para capitalizado
            6. Converte `Volume` para numÃ©rico
            7. Remove linhas onde `Oficina` ou `PerÃ­odo` sÃ£o NaN
            8. Preenche NaN em `Volume` com 0
            9. Remove duplicatas
            
            **Colunas Finais**: `Oficina`, `VeÃ­culo`, `PerÃ­odo`, `Volume`
            
            **DataFrame Criado**: `df_vol`
            """)
        
        with st.expander("ðŸ”— **CÃ©lula 6: Merge df_final â†” df_vol (Volume)**", expanded=False):
            st.markdown("""
            **Chave de Merge**: `['Oficina', 'PerÃ­odo', 'VeÃ­culo']` (COMPOSTA)
            **Tipo**: `left` (mantÃ©m todos os registros de df_final)
            
            **Processo**:
            1. Verifica se colunas de chave existem em ambos DataFrames
            2. Faz merge adicionando coluna `Volume` ao `df_final`
            3. Preenche NaN em `Volume` com 0 (se nÃ£o houver match)
            
            **Resultado**: `df_final` com coluna `Volume` adicionada
            """)
        
        with st.expander("ðŸ’¾ **CÃ©lula 7: Salvamento e ConsolidaÃ§Ã£o**", expanded=False):
            st.markdown("""
            **Arquivos Salvos (Pasta do Ano)**:
            1. `df_final.parquet` - Dados completos com rateio por veÃ­culo e volume
            2. `df_vol.parquet` - Dados de volume
            3. `df_ke5z_group.parquet` - Dados agrupados (se aplicÃ¡vel)
            
            **ConsolidaÃ§Ã£o do HistÃ³rico**:
            1. Carrega histÃ³rico existente (se existir):
                    - `dados/TC_Ext/historico_consolidado/df_final_historico.parquet`
                    - `dados/TC_Ext/historico_consolidado/df_vol_historico.parquet`
            2. Adiciona coluna `Ano` aos dados do ano atual
            3. Concatena dados do ano atual com histÃ³rico existente
            4. Remove duplicatas (se houver)
            5. Salva histÃ³rico atualizado:
                    - `dados/TC_Ext/historico_consolidado/df_final_historico.parquet`
                    - `dados/TC_Ext/historico_consolidado/df_vol_historico.parquet`
            
            **IMPORTANTE**: O histÃ³rico Ã© sempre **concatenado**, nunca substituÃ­do
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 3: tc_ext/notebooks/dados_BUD.ipynb - Dados BUDGET
        st.markdown("## ðŸ“ˆ NOTEBOOK tc_ext/notebooks/dados_BUD.ipynb - DADOS BUDGET {#dados-budget}")
        
        st.markdown("### DiferenÃ§as Principais em RelaÃ§Ã£o a tc_ext/notebooks/dados.ipynb")
        
        diferencas_bud = {
            "Aspecto": [
                "Guia de Dados Principais",
                "Guia de Rateio",
                "Guia de Volume",
                "Pasta de SaÃ­da",
                "Sufixo dos Arquivos",
                "Pasta de HistÃ³rico"
            ],
            "tc_ext/notebooks/dados.ipynb (REAL)": [
                '"Sapiens"',
                '"Rateio"',
                '"Volume"',
                "dados/TC_Ext/{ANO}/",
                "Sem sufixo",
                "dados/TC_Ext/historico_consolidado/"
            ],
            "tc_ext/notebooks/dados_BUD.ipynb (BUDGET)": [
                '"Voz de custo BDG"',
                '"Rateio BDG"',
                '"Volume BDG"',
                "dados/TC_Ext/{ANO}/BUD/",
                "_BUD (ex: df_final_BUD.parquet)",
                "dados/TC_Ext/historico_consolidado/BUD/"
            ]
        }
        
        st.dataframe(pd.DataFrame(diferencas_bud), use_container_width=True, hide_index=True)
        
        st.markdown("### Processo IdÃªntico")
        st.info("""
        **IMPORTANTE**: O processo de processamento, merges, cÃ¡lculos e consolidaÃ§Ã£o
        Ã© **IDÃŠNTICO** ao `tc_ext/notebooks/dados.ipynb`. A Ãºnica diferenÃ§a sÃ£o as guias lidas e os
        caminhos de saÃ­da. Todas as transformaÃ§Ãµes, relacionamentos e cÃ¡lculos seguem
        a mesma lÃ³gica.
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 4: Estrutura de Arquivos de Entrada
        st.markdown("## ðŸ“ ESTRUTURA DE ARQUIVOS DE ENTRADA {#estrutura-entrada}")
        
        st.markdown("### Arquivos NecessÃ¡rios")
        
        with st.expander("ðŸ“Š **Reporting fluxo anexo.xlsx**", expanded=False):
            st.markdown("""
            **LocalizaÃ§Ã£o**: `dados/TC_Ext/{ANO}/Reporting fluxo anexo.xlsx` ou raiz do projeto
            
            **Guias Utilizadas (tc_ext/notebooks/dados.ipynb - REAL)**:
            1. **"Sapiens"** (CÃ©lula 1)
               - CabeÃ§alho: Linha 1
               - Colunas: A atÃ© T (20 colunas)
               - Dados: Custos reais executados
            
            2. **"Rateio"** (CÃ©lula 3)
               - CabeÃ§alho: Segunda linha (apÃ³s linha de referÃªncia)
               - Colunas de meses: Janeiro a Dezembro
               - Dados: Percentuais de rateio por Oficina, VeÃ­culo e PerÃ­odo
            
            3. **"Volume"** (CÃ©lula 5)
               - CabeÃ§alho: Linha 51
               - Colunas de meses: Janeiro a Dezembro
               - Dados: Volumes por Oficina, VeÃ­culo e PerÃ­odo
            
            **Guias Utilizadas (tc_ext/notebooks/dados_BUD.ipynb - BUDGET)**:
            1. **"Voz de custo BDG"** (equivalente a "Sapiens")
            2. **"Rateio BDG"** (equivalente a "Rateio")
            3. **"Volume BDG"** (equivalente a "Volume")
            """)
        
        with st.expander("ðŸ“‹ **Dados SAPIENS.xlsx**", expanded=False):
            st.markdown("""
            **LocalizaÃ§Ã£o**: `dados/TC_Ext/{ANO}/Dados SAPIENS.xlsx` ou raiz do projeto
            
            **Guias Utilizadas**:
            1. **"Base conso"**
               - Colunas: `Type 04` (renomeado para `Custo`), `Type 07` (renomeado para `Account`)
               - PropÃ³sito: Mapear Account para tipo de custo (VariÃ¡vel/Fixo)
               - Chave de merge: `Account` (Type 07)
            
            **ObservaÃ§Ã£o**: Este arquivo Ã© usado tanto em `tc_ext/notebooks/dados.ipynb` quanto em `tc_ext/notebooks/dados_BUD.ipynb`
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 5: Relacionamentos e Merges
        st.markdown("## ðŸ”— RELACIONAMENTOS E MERGES {#relacionamentos}")
        
        st.markdown("### Resumo de Todos os Merges")
        
        resumo_merges = {
            "Merge": [
                "KE5Z â†” Base Conso",
                "KE5Z â†” Rateio",
                "KE5Z â†” Volume",
                "HistÃ³rico â†” Ano Atual"
            ],
            "Chave KE5Z": [
                "Account (Type 07)",
                "['Oficina', 'PerÃ­odo']",
                "['Oficina', 'PerÃ­odo', 'VeÃ­culo']",
                "N/A (concatenaÃ§Ã£o)"
            ],
            "Chave Externa": [
                "Account (Type 07)",
                "['Oficina', 'PerÃ­odo']",
                "['Oficina', 'PerÃ­odo', 'VeÃ­culo']",
                "N/A (concatenaÃ§Ã£o)"
            ],
            "Tipo": [
                "left",
                "left",
                "left",
                "concat"
            ],
            "Resultado": [
                "Coluna Custo (VariÃ¡vel/Fixo)",
                "Colunas de rateio por veÃ­culo (CC21%, CC22%, etc.)",
                "Coluna Volume",
                "HistÃ³rico consolidado com todos os anos"
            ]
        }
        
        st.dataframe(pd.DataFrame(resumo_merges), use_container_width=True, hide_index=True)
        
        st.markdown("### Detalhamento dos Merges")
        
        with st.expander("1. Merge KE5Z â†” Base Conso (Custo)", expanded=False):
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
            - Valores: `"VariÃ¡vel"` ou `"Fixo"`
            - Usado para cÃ¡lculos de Flex Bud e anÃ¡lises de custos fixos vs variÃ¡veis
            """)
        
        with st.expander("2. Merge KE5Z â†” Rateio (Percentuais por VeÃ­culo)", expanded=False):
            st.code("""
# Processamento do Rateio
df_rateio = pd.read_excel('Reporting fluxo anexo.xlsx', sheet_name='Rateio', header=None)
# ... processamento com melt() ...
df_pivot = df_rateio.pivot_table(
        index=['Oficina', 'PerÃ­odo'],
        columns='VeÃ­culo',
        values='Rateio',
        aggfunc='mean'
).reset_index()

# Renomear colunas de veÃ­culos para adicionar %
veiculos_cols = ['CC21', 'CC22', 'CC24', 'CC24 5L', 'CC24 7L', 'J516']
rename_dict = {col: f"{col}%" for col in veiculos_cols}
df_pivot = df_pivot.rename(columns=rename_dict)

# Merge
df_final = pd.merge(df_KE5Z, df_pivot, on=['Oficina', 'PerÃ­odo'], how='left')
            """, language="python")
            
            st.markdown("""
            **Resultado**: Adiciona colunas de percentuais por veÃ­culo
            - `CC21%`, `CC22%`, `CC24%`, `CC24 5L%`, `CC24 7L%`, `J516%`
            - Valores: Percentuais (0.0 a 1.0 ou 0% a 100%)
            - Usado para calcular valores por veÃ­culo: `CC21 = CC21% * Valor`
            """)
        
        with st.expander("3. Merge df_final â†” Volume", expanded=False):
            st.code("""
# Processamento do Volume
df_vol = pd.read_excel('Reporting fluxo anexo.xlsx', sheet_name='Volume', header=50)
# ... processamento com melt() ...
# Colunas finais: Oficina, VeÃ­culo, PerÃ­odo, Volume

# Merge
df_final = pd.merge(df_final, df_vol, on=['Oficina', 'PerÃ­odo', 'VeÃ­culo'], how='left')
df_final['Volume'] = df_final['Volume'].fillna(0)
            """, language="python")
            
            st.markdown("""
            **Resultado**: Adiciona coluna `Volume` ao `df_final`
            - Valores: Volumes numÃ©ricos por veÃ­culo
            - Usado para cÃ¡lculos de CPU (Custo por Unidade)
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 6: Colunas e Estrutura Final
        st.markdown("## ðŸ“Š COLUNAS E ESTRUTURA FINAL {#colunas-finais}")
        
        st.markdown("### Colunas do DataFrame Final (df_final.parquet)")
        
        colunas_finais = {
            "Coluna": [
                "Mes", "PerÃ­odo", "Ano",
                "NÂºconta", "Centrocst", "NÂºdoc.ref.", "Dt.lÃ§to.",
                "Valor", "QTD", "Volume",
                "Type 05", "Type 06", "Account", "Custo",
                "USI", "Oficina",
                "Doc.compra", "Texto breve",
                "Fornecedor", "Material", "UsuÃ¡rio", "Fornec.", "Tipo",
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
                "Sapiens", "Sapiens", "Adicionado na consolidaÃ§Ã£o",
                "Sapiens", "Sapiens", "Sapiens", "Sapiens",
                "Sapiens", "Sapiens", "Volume (merge)",
                "Sapiens", "Sapiens", "Sapiens", "Base conso (merge)",
                "Sapiens", "Sapiens",
                "Sapiens", "Sapiens",
                "Sapiens", "Sapiens", "Sapiens", "Sapiens", "Sapiens",
                "Calculado (CC21% * Valor)", "Calculado", "Calculado", "Calculado", "Calculado", "Calculado",
                "Calculado (soma dos %)"
            ],
            "DescriÃ§Ã£o": [
                "MÃªs numÃ©rico (1-12)", "MÃªs por extenso (Janeiro, etc.)", "Ano do registro",
                "CÃ³digo da conta contÃ¡bil", "Centro de custo", "NÃºmero documento referÃªncia", "Data de lanÃ§amento",
                "Valor monetÃ¡rio do custo", "Quantidade", "Volume do veÃ­culo",
                "ClassificaÃ§Ã£o Type 05", "ClassificaÃ§Ã£o Type 06", "Account (Type 07)", "Tipo de custo (VariÃ¡vel/Fixo)",
                "Unidade de negÃ³cio", "Nome da oficina",
                "Documento de compra", "DescriÃ§Ã£o breve do material",
                "Nome do fornecedor", "CÃ³digo do material", "UsuÃ¡rio", "CÃ³digo fornecedor", "Tipo de lanÃ§amento",
                "Valor rateado para CC21", "Valor rateado para CC22", "Valor rateado para CC24", "Valor rateado para CC24 5L", "Valor rateado para CC24 7L", "Valor rateado para J516",
                "Soma de todos os percentuais (validaÃ§Ã£o)"
            ]
        }
        
        st.dataframe(pd.DataFrame(colunas_finais), use_container_width=True, hide_index=True)
        
        st.markdown("### Colunas do DataFrame de Volume (df_vol.parquet)")
        
        colunas_volume = {
            "Coluna": ["Oficina", "VeÃ­culo", "PerÃ­odo", "Volume"],
            "Tipo": ["object", "object", "object", "float64"],
            "DescriÃ§Ã£o": [
                "Nome da oficina",
                "CÃ³digo do veÃ­culo (CC21, CC22, CC24, CC24 5L, CC24 7L, J516)",
                "MÃªs por extenso (Janeiro, Fevereiro, etc.)",
                "Volume numÃ©rico do veÃ­culo no perÃ­odo"
            ]
        }
        
        st.dataframe(pd.DataFrame(colunas_volume), use_container_width=True, hide_index=True)
        
        st.markdown("### Relacionamento entre Colunas")
        
        st.markdown("""
        **Chaves PrimÃ¡rias para Merges**:
        - `Account` (Type 07) â†’ Merge com Base Conso
        - `['Oficina', 'PerÃ­odo']` â†’ Merge com Rateio
        - `['Oficina', 'PerÃ­odo', 'VeÃ­culo']` â†’ Merge com Volume
        
        **Colunas Calculadas**:
        - `CC21 = CC21% * Valor` (e similares para outros veÃ­culos)
        - `Soma_Percentuais = CC21% + CC22% + CC24% + CC24 5L% + CC24 7L% + J516%`
        - `CPU = Valor / Volume` (calculado no app.py, nÃ£o no notebook)
        
        **NormalizaÃ§Ãµes CrÃ­ticas**:
        - `PerÃ­odo`: Sempre capitalizado (Janeiro, Fevereiro, etc.)
        - `Account`: Mantido como string/object
        - `Volume`: Sempre numÃ©rico (float64), NaN preenchido com 0
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 7: ConsolidaÃ§Ã£o do HistÃ³rico
        st.markdown("## ðŸ“š CONSOLIDAÃ‡ÃƒO DO HISTÃ“RICO {#consolidacao}")
        
        st.markdown("### Processo de ConsolidaÃ§Ã£o")
        
        st.markdown("""
        **Objetivo**: Manter um histÃ³rico completo de todos os anos processados
        
        **Processo**:
        1. **Verificar histÃ³rico existente**:
              - Tenta carregar `dados/TC_Ext/historico_consolidado/df_final_historico.parquet`
           - Se nÃ£o existir, cria DataFrame vazio
        
        2. **Adicionar coluna Ano**:
           - Adiciona `Ano = ANO_ATUAL` aos dados do ano atual
           - Garante que cada registro tenha identificaÃ§Ã£o do ano
        
        3. **ConcatenaÃ§Ã£o**:
           - Concatena dados do ano atual com histÃ³rico existente
           - Usa `pd.concat([df_historico, df_ano_atual], ignore_index=True)`
        
        4. **ValidaÃ§Ã£o**:
           - Verifica se `Volume` Ã© sempre numÃ©rico
           - Garante tipos de dados consistentes
        
        5. **Salvamento**:
           - Salva histÃ³rico atualizado
           - MantÃ©m histÃ³rico sempre completo
        
        **IMPORTANTE**: 
        - O histÃ³rico Ã© **sempre concatenado**, nunca substituÃ­do
        - Permite anÃ¡lises multi-anos no dashboard
        - O sistema prioriza o histÃ³rico consolidado para carregar dados
        """)
        
        st.markdown("### Estrutura do HistÃ³rico")
        
        st.code("""
        dados/TC_Ext/historico_consolidado/
        â”œâ”€â”€ df_final_historico.parquet      # Todos os anos de custos (REAL)
        â”œâ”€â”€ df_vol_historico.parquet        # Todos os anos de volumes
        â”œâ”€â”€ df_ke5z_historico.parquet       # Dados KE5Z agrupados
        â””â”€â”€ BUD/
            â”œâ”€â”€ df_final_historico_BUD.parquet  # Todos os anos de custos (BUDGET)
            â”œâ”€â”€ df_vol_historico_BUD.parquet    # Todos os anos de volumes (BUDGET)
            â””â”€â”€ df_ke5z_historico_BUD.parquet   # Dados KE5Z agrupados (BUDGET)
        """, language="text")
        
        st.markdown("---")
        
        # SeÃ§Ã£o 8: Arquivos de SaÃ­da
        st.markdown("## ðŸ’¾ ARQUIVOS DE SAÃDA {#arquivos-saida}")
        
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
            "LocalizaÃ§Ã£o": [
                "dados/TC_Ext/{ANO}/",
                "dados/TC_Ext/{ANO}/",
                "dados/TC_Ext/{ANO}/",
                "dados/TC_Ext/historico_consolidado/",
                "dados/TC_Ext/historico_consolidado/",
                "dados/TC_Ext/historico_consolidado/"
            ],
            "ConteÃºdo": [
                "Dados completos com rateio por veÃ­culo e volume",
                "Dados de volume por Oficina, VeÃ­culo e PerÃ­odo",
                "Dados agrupados KE5Z",
                "HistÃ³rico consolidado de todos os anos (REAL)",
                "HistÃ³rico consolidado de volumes",
                "HistÃ³rico consolidado KE5Z"
            ],
            "Uso": [
                "Dashboard principal (app.py)",
                "CÃ¡lculos de CPU e anÃ¡lises de volume",
                "AnÃ¡lises especÃ­ficas",
                "AnÃ¡lises multi-anos",
                "AnÃ¡lises multi-anos de volume",
                "AnÃ¡lises histÃ³ricas KE5Z"
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
            "LocalizaÃ§Ã£o": [
                "dados/TC_Ext/{ANO}/BUD/",
                "dados/TC_Ext/{ANO}/BUD/",
                "dados/TC_Ext/{ANO}/BUD/",
                "dados/TC_Ext/historico_consolidado/BUD/",
                "dados/TC_Ext/historico_consolidado/BUD/",
                "dados/TC_Ext/historico_consolidado/BUD/"
            ],
            "ConteÃºdo": [
                "Dados de Budget com rateio por veÃ­culo e volume",
                "Dados de volume de Budget",
                "Dados agrupados KE5Z (Budget)",
                "HistÃ³rico consolidado de todos os anos (BUDGET)",
                "HistÃ³rico consolidado de volumes (Budget)",
                "HistÃ³rico consolidado KE5Z (Budget)"
            ],
            "Uso": [
                "ComparaÃ§Ãµes Real vs Budget",
                "AnÃ¡lises de volume Budget",
                "AnÃ¡lises especÃ­ficas Budget",
                "AnÃ¡lises multi-anos Budget",
                "AnÃ¡lises multi-anos de volume Budget",
                "AnÃ¡lises histÃ³ricas KE5Z Budget"
            ]
        }
        
        st.dataframe(pd.DataFrame(arquivos_saida_bud), use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 9: Fluxo Completo
        st.markdown("## ðŸ”„ FLUXO COMPLETO {#fluxo-completo}")
        
        st.markdown("### Diagrama de Fluxo - tc_ext/notebooks/dados.ipynb")
        
        st.code("""
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  ConfiguraÃ§Ã£o (CÃ©lula 0)           â”‚
        â”‚  - Define ANO_ATUAL                 â”‚
        â”‚  - Cria pastas                      â”‚
        â”‚  - Verifica arquivos                â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Leitura SAPIENS (CÃ©lula 1)        â”‚
        â”‚  - Reporting fluxo anexo.xlsx       â”‚
        â”‚  - Guia "Sapiens"                   â”‚
        â”‚  - Cria df_KE5Z                     â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Merge Base Conso (CÃ©lula 2)       â”‚
        â”‚  - Dados SAPIENS.xlsx                â”‚
        â”‚  - Guia "Base conso"                 â”‚
        â”‚  - Adiciona coluna Custo             â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Processamento Rateio (CÃ©lula 3)   â”‚
        â”‚  - Reporting fluxo anexo.xlsx         â”‚
        â”‚  - Guia "Rateio"                     â”‚
        â”‚  - Transforma meses em linhas         â”‚
        â”‚  - Cria df (Oficina, VeÃ­culo,        â”‚
        â”‚    PerÃ­odo, Rateio)                  â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Merge + CÃ¡lculo VeÃ­culos (CÃ©lula 4)â”‚
        â”‚  - Merge KE5Z â†” Rateio               â”‚
        â”‚  - Pivot: VeÃ­culo â†’ Colunas          â”‚
        â”‚  - Calcula: CC21 = CC21% * Valor     â”‚
        â”‚  - Cria df_final                     â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Processamento Volume (CÃ©lula 5)   â”‚
        â”‚  - Reporting fluxo anexo.xlsx       â”‚
        â”‚  - Guia "Volume"                     â”‚
        â”‚  - Transforma meses em linhas         â”‚
        â”‚  - Cria df_vol                       â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Merge Volume (CÃ©lula 6)           â”‚
        â”‚  - Merge df_final â†” df_vol           â”‚
        â”‚  - Adiciona coluna Volume             â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚
                       â–¼
        â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â”‚  Salvamento + ConsolidaÃ§Ã£o (CÃ©lula 7)â”‚
        â”‚  - Salva df_final.parquet            â”‚
        â”‚  - Salva df_vol.parquet               â”‚
        â”‚  - Carrega histÃ³rico                  â”‚
        â”‚  - Concatena com ano atual            â”‚
        â”‚  - Salva histÃ³rico atualizado         â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
        """, language="text")
        
        st.markdown("### SequÃªncia de OperaÃ§Ãµes Detalhada")
        
        operacoes_detalhadas = [
            "**CÃ©lula 0**: ConfiguraÃ§Ã£o - Define ano, cria pastas, verifica arquivos de entrada",
            "**CÃ©lula 1**: Leitura SAPIENS - LÃª guia 'Sapiens' (20 colunas), cria df_KE5Z com dados de custos",
            "**CÃ©lula 2**: Merge Base Conso - Adiciona coluna 'Custo' (VariÃ¡vel/Fixo) usando Account como chave",
            "**CÃ©lula 3**: Processamento Rateio - LÃª guia 'Rateio', transforma meses em linhas (melt), cria df com Oficina, VeÃ­culo, PerÃ­odo, Rateio",
            "**CÃ©lula 4**: Merge Rateio + CÃ¡lculo - Merge KE5Z â†” Rateio, pivot de VeÃ­culo para colunas, calcula valores por veÃ­culo (CC21, CC22, etc.), cria df_final",
            "**CÃ©lula 5**: Processamento Volume - LÃª guia 'Volume' (header=50), transforma meses em linhas, cria df_vol com Oficina, VeÃ­culo, PerÃ­odo, Volume",
            "**CÃ©lula 6**: Merge Volume - Merge df_final â†” df_vol usando ['Oficina', 'PerÃ­odo', 'VeÃ­culo'], adiciona coluna Volume",
            "**CÃ©lula 7**: Salvamento - Salva df_final.parquet, df_vol.parquet na pasta do ano, carrega histÃ³rico, concatena, salva histÃ³rico consolidado"
        ]
        
        for op in operacoes_detalhadas:
            st.markdown(f"- {op}")
        
        st.markdown("---")
        
        # SeÃ§Ã£o 10: Tratamento de Erros
        st.markdown("## âš ï¸ TRATAMENTO DE ERROS {#tratamento-erros}")
        
        st.markdown("### Erros Comuns e SoluÃ§Ãµes")
        
        with st.expander("1. Arquivo NÃ£o Encontrado", expanded=False):
            st.markdown("""
            **Sintoma**: `FileNotFoundError` ao tentar ler arquivo Excel
            
            **SoluÃ§Ãµes**:
            - Verificar se arquivo estÃ¡ em `dados/TC_Ext/{ANO}/` ou na raiz do projeto
            - Verificar nomes exatos: `Dados SAPIENS.xlsx` e `Reporting fluxo anexo.xlsx`
            - O notebook tenta copiar da raiz para pasta do ano automaticamente
            """)
        
        with st.expander("2. Guia NÃ£o Encontrada", expanded=False):
            st.markdown("""
            **Sintoma**: `ValueError: Worksheet named 'X' not found`
            
            **SoluÃ§Ãµes**:
            - Verificar nomes exatos das guias (case-sensitive):
              - `tc_ext/notebooks/dados.ipynb`: "Sapiens", "Rateio", "Volume"
              - `tc_ext/notebooks/dados_BUD.ipynb`: "Voz de custo BDG", "Rateio BDG", "Volume BDG"
            - Verificar se guias existem no arquivo Excel
            """)
        
        with st.expander("3. Coluna NÃ£o Encontrada ApÃ³s Merge", expanded=False):
            st.markdown("""
            **Sintoma**: `KeyError: 'Coluna X'` apÃ³s merge
            
            **SoluÃ§Ãµes**:
            - Verificar se chaves de merge existem em ambos DataFrames
            - Verificar normalizaÃ§Ã£o de `PerÃ­odo` (deve estar capitalizado)
            - Verificar tipos de dados das chaves (devem ser compatÃ­veis)
            - Verificar se merge foi feito com chaves corretas
            """)
        
        with st.expander("4. Volume NaN ou Zerado", expanded=False):
            st.markdown("""
            **Sintoma**: Coluna Volume com muitos NaN ou zeros
            
            **SoluÃ§Ãµes**:
            - Verificar se merge foi feito com chave composta correta: `['Oficina', 'PerÃ­odo', 'VeÃ­culo']`
            - Verificar se dados de volume existem para a combinaÃ§Ã£o Oficina+PerÃ­odo+VeÃ­culo
            - Verificar normalizaÃ§Ã£o de `PerÃ­odo` (deve estar capitalizado em ambos DataFrames)
            - O notebook preenche NaN com 0 automaticamente
            """)
        
        with st.expander("5. Percentuais de Rateio NÃ£o Somam 100%", expanded=False):
            st.markdown("""
            **Sintoma**: `Soma_Percentuais` diferente de 1.0 (ou 100%)
            
            **SoluÃ§Ãµes**:
            - Verificar se todos os veÃ­culos estÃ£o incluÃ­dos no rateio
            - Verificar se hÃ¡ veÃ­culos nÃ£o mapeados
            - Verificar se pivot foi feito corretamente (aggfunc='mean')
            - ValidaÃ§Ã£o: `Soma_Percentuais` deve estar prÃ³ximo de 1.0
            """)
        
        with st.expander("6. HistÃ³rico NÃ£o Atualizado", expanded=False):
            st.markdown("""
            **Sintoma**: HistÃ³rico nÃ£o inclui dados do ano atual apÃ³s processamento
            
            **SoluÃ§Ãµes**:
            - Verificar se coluna `Ano` foi adicionada aos dados do ano atual
            - Verificar se concatenaÃ§Ã£o foi executada corretamente
            - Verificar se arquivo de histÃ³rico foi salvo apÃ³s concatenaÃ§Ã£o
            - Verificar permissÃµes de escrita na pasta `dados/TC_Ext/historico_consolidado/`
            """)
        
        st.markdown("### ValidaÃ§Ãµes Implementadas")
        
        st.markdown("""
        **ValidaÃ§Ãµes AutomÃ¡ticas**:
        1. **ValidaÃ§Ã£o de Arquivos**: Verifica existÃªncia antes de processar
        2. **ValidaÃ§Ã£o de Colunas**: Verifica se colunas essenciais existem antes de merge
        3. **ValidaÃ§Ã£o de Volume**: Garante que Volume seja sempre numÃ©rico
        4. **ValidaÃ§Ã£o de PerÃ­odo**: Normaliza para formato capitalizado
        5. **ValidaÃ§Ã£o de HistÃ³rico**: Verifica tipos de dados ao carregar histÃ³rico
        6. **ValidaÃ§Ã£o de Soma**: Calcula `Soma_Percentuais` para validar rateios
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 11: Checklist para ManutenÃ§Ã£o
        st.markdown("## âœ… CHECKLIST PARA MANUTENÃ‡ÃƒO {#checklist}")
        
        st.markdown("### Antes de Modificar os Notebooks")
        
        checklist_antes = [
            "Verificar se estrutura de pastas estÃ¡ correta",
            "Verificar se nomes de guias estÃ£o corretos",
            "Verificar se chaves de merge estÃ£o corretas",
            "Verificar se tipos de dados estÃ£o consistentes",
            "Verificar se normalizaÃ§Ã£o de PerÃ­odo estÃ¡ funcionando",
            "Verificar se cÃ¡lculo de veÃ­culos estÃ¡ correto",
            "Verificar se consolidaÃ§Ã£o de histÃ³rico estÃ¡ funcionando"
        ]
        
        for item in checklist_antes:
            st.markdown(f"- [ ] {item}")
        
        st.markdown("### Ao Modificar")
        
        checklist_modificar = [
            "Manter mesma estrutura de chaves de merge",
            "Manter normalizaÃ§Ã£o de PerÃ­odo (capitalizado)",
            "Manter tipos de dados consistentes (Volume sempre numÃ©rico)",
            "Manter lÃ³gica de cÃ¡lculo de veÃ­culos (CC21 = CC21% * Valor)",
            "Manter processo de consolidaÃ§Ã£o de histÃ³rico (concatenaÃ§Ã£o, nÃ£o substituiÃ§Ã£o)",
            "Testar com dados de um ano antes de processar todos",
            "Validar que Volume nÃ£o estÃ¡ sendo zerado incorretamente",
            "Validar que Soma_Percentuais estÃ¡ prÃ³ximo de 1.0"
        ]
        
        for item in checklist_modificar:
            st.markdown(f"- [ ] {item}")
        
        st.markdown("### ApÃ³s Modificar")
        
        checklist_depois = [
            "Verificar se arquivos Parquet foram gerados corretamente",
            "Verificar se histÃ³rico foi atualizado",
            "Verificar se Volume estÃ¡ presente e numÃ©rico",
            "Verificar se colunas de veÃ­culos foram calculadas",
            "Verificar se nÃ£o hÃ¡ erros de tipo de dados",
            "Testar carregamento no app.py",
            "Validar que dados aparecem corretamente no dashboard"
        ]
        
        for item in checklist_depois:
            st.markdown(f"- [ ] {item}")
        
        st.markdown("### Regras CrÃ­ticas que NUNCA Devem Ser Alteradas")
        
        st.warning("""
        **âš ï¸ ATENÃ‡ÃƒO**: As seguintes regras sÃ£o CRÃTICAS e nÃ£o devem ser alteradas sem
        anÃ¡lise profunda, pois podem quebrar todo o sistema:
        
        1. **Chaves de Merge**: `['Oficina', 'PerÃ­odo']` para Rateio e `['Oficina', 'PerÃ­odo', 'VeÃ­culo']` para Volume
        2. **NormalizaÃ§Ã£o de PerÃ­odo**: Sempre capitalizado (Janeiro, Fevereiro, etc.)
        3. **CÃ¡lculo de VeÃ­culos**: `CC21 = CC21% * Valor` (e similares)
        4. **ConsolidaÃ§Ã£o de HistÃ³rico**: Sempre concatenar, nunca substituir
        5. **Tipo de Volume**: Sempre numÃ©rico (float64), nunca object
        6. **Estrutura de Pastas (TC Ext)**: `dados/TC_Ext/{ANO}/` para ano especÃ­fico, `dados/TC_Ext/historico_consolidado/` para histÃ³rico
        7. **Sufixo BUD**: Arquivos de Budget sempre com sufixo `_BUD` e em pasta `BUD/`
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o Final: Notas Importantes
        st.markdown("## ðŸ“ NOTAS IMPORTANTES PARA IA")
        
        st.markdown("### Quando Fazer ManutenÃ§Ã£o")
        
        st.markdown("""
        **FaÃ§a manutenÃ§Ã£o quando**:
        - Estrutura dos arquivos Excel de entrada mudar
        - Novas colunas forem adicionadas aos dados
        - Novos veÃ­culos forem adicionados ao sistema
        - LÃ³gica de rateio mudar
        - Estrutura de pastas precisar ser alterada
        
        **NÃƒO faÃ§a manutenÃ§Ã£o quando**:
        - Apenas dados novos forem adicionados (processe normalmente)
        - Apenas valores mudarem (processe normalmente)
        - Apenas anos novos forem processados (processe normalmente)
        """)
        
        st.markdown("### Como Fazer ManutenÃ§Ã£o Segura")
        
        st.markdown("""
        1. **Sempre teste primeiro**: Processe um ano de teste antes de processar todos
        2. **Mantenha backups**: FaÃ§a backup dos arquivos Parquet antes de modificar
        3. **Valide resultados**: Verifique se Volume, valores por veÃ­culo e histÃ³rico estÃ£o corretos
        4. **Documente mudanÃ§as**: Adicione comentÃ¡rios explicando alteraÃ§Ãµes
        5. **Mantenha consistÃªncia**: Se alterar `tc_ext/notebooks/dados.ipynb`, altere `tc_ext/notebooks/dados_BUD.ipynb` da mesma forma
        6. **Valide merges**: Sempre verifique se chaves de merge existem antes de fazer merge
        7. **Valide tipos**: Sempre verifique tipos de dados apÃ³s transformaÃ§Ãµes
        """)
        
        st.markdown("### Estrutura de DependÃªncias")
        
        st.code("""
        tc_ext/notebooks/dados.ipynb depende de:
        â”œâ”€â”€ Reporting fluxo anexo.xlsx
        â”‚   â”œâ”€â”€ Guia "Sapiens" (dados principais)
        â”‚   â”œâ”€â”€ Guia "Rateio" (percentuais por veÃ­culo)
        â”‚   â””â”€â”€ Guia "Volume" (volumes por veÃ­culo)
        â””â”€â”€ Dados SAPIENS.xlsx
            â””â”€â”€ Guia "Base conso" (mapeamento Custo)
        
        tc_ext/notebooks/dados_BUD.ipynb depende de:
        â”œâ”€â”€ Reporting fluxo anexo.xlsx
        â”‚   â”œâ”€â”€ Guia "Voz de custo BDG" (dados principais)
        â”‚   â”œâ”€â”€ Guia "Rateio BDG" (percentuais por veÃ­culo)
        â”‚   â””â”€â”€ Guia "Volume BDG" (volumes por veÃ­culo)
        â””â”€â”€ Dados SAPIENS.xlsx
            â””â”€â”€ Guia "Base conso" (mapeamento Custo)
        """, language="text")
        
        st.markdown("---")
        
        st.success("""
        **âœ… Este guia contÃ©m todas as informaÃ§Ãµes necessÃ¡rias para fazer manutenÃ§Ã£o**
        nos notebooks de extraÃ§Ã£o sem quebrar o sistema. Sempre consulte este guia
        antes de fazer alteraÃ§Ãµes e siga o checklist de validaÃ§Ã£o.
        """)
    
    # ==========================================
    # CAPÃTULO 2: FUNCIONAMENTO DA ATUALIZAÃ‡ÃƒO E EXTRAÃ‡ÃƒO
    # ==========================================
    
    with st.expander("ðŸ”„ **CapÃ­tulo 2: Funcionamento da AtualizaÃ§Ã£o e ExtraÃ§Ã£o**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">ðŸ”„ CapÃ­tulo 2: Funcionamento da AtualizaÃ§Ã£o e ExtraÃ§Ã£o</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Processo Completo de AtualizaÃ§Ã£o de Dados - Passo a Passo Detalhado
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # SeÃ§Ã£o 1: VisÃ£o Geral do Processo de AtualizaÃ§Ã£o
        st.markdown("## ðŸŽ¯ VISÃƒO GERAL DO PROCESSO DE ATUALIZAÃ‡ÃƒO {#visao-atualizacao}")
        
        st.markdown("""
        Este capÃ­tulo descreve **como funciona o processo completo de atualizaÃ§Ã£o de dados**,
        desde a preparaÃ§Ã£o dos arquivos atÃ© a execuÃ§Ã£o do processamento. Entender este fluxo
        Ã© essencial para realizar atualizaÃ§Ãµes corretamente, especialmente quando se trabalha
        com novos anos ou quando se precisa atualizar arquivos existentes.
        """)
        
        st.info("""
        **ðŸ’¡ Importante**: O sistema foi projetado para ser flexÃ­vel e permitir atualizaÃ§Ãµes
        de diferentes formas: atravÃ©s de upload direto na interface, colocando arquivos na
        raiz do projeto, ou organizando-os nas pastas do ano. O sistema busca automaticamente
        os arquivos na ordem de prioridade definida.
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 2: Ordem CronolÃ³gica dos Eventos
        st.markdown("## â±ï¸ ORDEM CRONOLÃ“GICA DOS EVENTOS {#ordem-cronologica}")
        
        st.markdown("### SequÃªncia Completa do Processo")
        
        with st.expander("**1ï¸âƒ£ SeleÃ§Ã£o do Ano e Tipo de ExtraÃ§Ã£o**", expanded=False):
            st.markdown("""
            **Onde**: PÃ¡gina "5 - ExtraÃ§Ã£o de Dados" (Streamlit)
            
            **Processo**:
            1. UsuÃ¡rio seleciona o **ano** que deseja processar (ex: 2024, 2025, 2026)
            2. UsuÃ¡rio seleciona o **tipo de extraÃ§Ã£o**:
               - ðŸ“Š **Dados REAIS** (tc_ext/notebooks/dados.ipynb) - Processa custos reais executados
               - ðŸ’° **Dados BUDGET** (tc_ext/notebooks/dados_BUD.ipynb) - Processa dados de orÃ§amento
               - ðŸ”„ **Ambos** - Processa REAIS e BUDGET sequencialmente
            
            **Resultado**: Sistema sabe qual ano processar e quais notebooks executar
            """)
        
        with st.expander("**2ï¸âƒ£ VerificaÃ§Ã£o e PreparaÃ§Ã£o de Arquivos**", expanded=False):
            st.markdown("""
            **Onde**: Antes do processamento, na aba "ValidaÃ§Ã£o de Arquivos"
            
            **Processo**:
            1. Sistema verifica se os arquivos necessÃ¡rios jÃ¡ existem
            2. Sistema mostra avisos se arquivos jÃ¡ existem (para evitar sobrescrita acidental)
            3. UsuÃ¡rio pode fazer upload de arquivos diretamente na interface
            
            **Arquivos NecessÃ¡rios para Dados REAIS**:
            - `Dados SAPIENS.xlsx` - Base de dados SAPIENS com classificaÃ§Ã£o de custos
            - `Reporting fluxo anexo.xlsx` - Dados de custos, rateio e volumes
            
            **Arquivos NecessÃ¡rios para Dados BUDGET**:
            - `Dados SAPIENS.xlsx` - Base de dados SAPIENS (mesmo arquivo ou versÃ£o Budget)
            - `Reporting fluxo anexo.xlsx` - Dados de Budget (guias "Voz de custo BDG", "Rateio BDG", "Volume BDG")
            """)
        
        with st.expander("**3ï¸âƒ£ Sistema de Upload de Arquivos (Opcional)**", expanded=False):
            st.markdown("""
            **Onde**: Aba "ValidaÃ§Ã£o de Arquivos" â†’ SeÃ§Ã£o "ðŸ“¤ Upload de Arquivos"
            
            **Processo**:
            1. UsuÃ¡rio clica em "Browse Files" para selecionar arquivo
            2. **ANTES do upload**: Sistema verifica se arquivo jÃ¡ existe na pasta de destino
               - Se existe: Mostra aviso âš ï¸ informando que serÃ¡ sobrescrito
               - Se nÃ£o existe: Permite upload direto
            3. UsuÃ¡rio seleciona arquivo do computador
            4. **APÃ“S seleÃ§Ã£o**: Sistema verifica novamente se arquivo existe
               - Se existe: Mostra aviso e botÃ£o "ðŸ”„ Confirmar Sobrescrita"
               - Se nÃ£o existe: Salva automaticamente
            5. Arquivo Ã© salvo em: `dados/TC_Ext/{ANO_SELECIONADO}/Nome_do_Arquivo.xlsx`
            6. PÃ¡gina recarrega automaticamente (`st.rerun()`) para atualizar status
            
            **Vantagens do Upload**:
            - NÃ£o precisa colocar arquivos na raiz do projeto
            - Arquivos sÃ£o organizados automaticamente na pasta do ano
            - Sistema cria a pasta do ano automaticamente se nÃ£o existir
            - Avisos preventivos evitam sobrescrita acidental
            """)
        
        with st.expander("**4ï¸âƒ£ CriaÃ§Ã£o da Estrutura de Pastas**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o `configurar_ano()` ou `configurar_ano_bud()` nos mÃ³dulos Python
            
            **Processo** (executado automaticamente ao iniciar processamento):
                1. **Cria pasta do ano**: `dados/TC_Ext/{ANO}/`
                    - Exemplo: `dados/TC_Ext/2024/` para ano 2024
                    - Exemplo: `dados/TC_Ext/2026/` para ano 2026 (novo ano)
            
                2. **Para dados REAIS**: Cria apenas `dados/TC_Ext/{ANO}/`
            
                3. **Para dados BUDGET**: Cria tambÃ©m `dados/TC_Ext/{ANO}/BUD/`
                          - Estrutura: `dados/TC_Ext/2024/BUD/` para **outputs** de Budget
            
            4. **Cria pastas de histÃ³rico** (se nÃ£o existirem):
                    - `dados/TC_Ext/historico_consolidado/` - Para dados REAIS
                    - `dados/TC_Ext/historico_consolidado/BUD/` - Para dados BUDGET
            
            **IMPORTANTE**: 
            - Pastas sÃ£o criadas automaticamente, mesmo que nÃ£o existam
            - Se a pasta jÃ¡ existe, nÃ£o hÃ¡ problema (nÃ£o sobrescreve)
            - Sistema usa `os.makedirs(pasta, exist_ok=True)` para criar com seguranÃ§a
            """)
        
        with st.expander("**5ï¸âƒ£ Sistema de Busca de Arquivos**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o `encontrar_arquivo()` nos mÃ³dulos de processamento
            
            **Ordem de Prioridade de Busca** (do mais prioritÃ¡rio ao menos prioritÃ¡rio):
            
            **Para Dados REAIS**:
                1. **Primeira opÃ§Ã£o**: `dados/TC_Ext/{ANO}/Nome_do_Arquivo.xlsx`
                    - Exemplo: `dados/TC_Ext/2024/Dados SAPIENS.xlsx`
               - **Esta Ã© a pasta preferencial!** Arquivos aqui tÃªm prioridade mÃ¡xima
            
            2. **Segunda opÃ§Ã£o**: `./Nome_do_Arquivo.xlsx` (raiz do projeto)
               - Exemplo: `./Dados SAPIENS.xlsx`
               - Usado quando arquivo nÃ£o estÃ¡ na pasta do ano
            
            **Para Dados BUDGET**:
                1. **Primeira opÃ§Ã£o**: `dados/TC_Ext/{ANO}/Nome_do_Arquivo.xlsx`
                    - Exemplo: `dados/TC_Ext/2024/Dados SAPIENS.xlsx`

                2. **Segunda opÃ§Ã£o**: `./Nome_do_Arquivo.xlsx` (raiz do projeto)

                *(Compatibilidade/legado)*: se existir arquivo em `dados/TC_Ext/{ANO}/BUD/`, ele pode ser **copiado** para `dados/TC_Ext/{ANO}/`.
            
            **Comportamento**:
            - Sistema busca na ordem acima e usa o **primeiro arquivo encontrado**
            - Se arquivo nÃ£o for encontrado em nenhum local, sistema retorna erro
            - Se arquivo for encontrado na raiz, pode ser copiado para pasta do ano (dependendo da configuraÃ§Ã£o)
            
            **Exemplo PrÃ¡tico - Processando 2026 pela primeira vez**:
            ```
            1. Sistema cria: dados/TC_Ext/2026/
            2. Sistema busca: dados/TC_Ext/2026/Dados SAPIENS.xlsx â†’ âŒ NÃ£o encontrado
            3. Sistema busca: ./Dados SAPIENS.xlsx â†’ âœ… Encontrado na raiz
            4. Sistema usa: ./Dados SAPIENS.xlsx (da raiz)
            5. Arquivos de saÃ­da sÃ£o salvos em: dados/TC_Ext/2026/
            ```
            """)
        
        with st.expander("**6ï¸âƒ£ ExecuÃ§Ã£o do Processamento**", expanded=False):
            st.markdown("""
            **Onde**: Aba "Executar Processamento" â†’ BotÃµes de execuÃ§Ã£o
            
            **Processo**:
            1. UsuÃ¡rio clica em botÃ£o de execuÃ§Ã£o:
               - "ðŸš€ Executar tc_ext/notebooks/dados.ipynb" (para REAIS)
               - "ðŸš€ Executar tc_ext/notebooks/dados_BUD.ipynb" (para BUDGET)
               - "ðŸš€ Executar Ambos" (para REAIS e BUDGET)
            
            2. Sistema chama funÃ§Ã£o de processamento correspondente:
               - `processar_completo()` para dados REAIS
               - `processar_completo_bud()` para dados BUDGET
            
            3. **ConfiguraÃ§Ã£o inicial**:
               - Chama `configurar_ano()` ou `configurar_ano_bud()`
               - Cria estrutura de pastas
               - Busca arquivos usando `encontrar_arquivo()`
               - Valida se arquivos existem
            
            4. **Processamento dos dados**:
               - LÃª arquivos Excel das guias corretas
               - Faz merges e transformaÃ§Ãµes
               - Calcula valores por veÃ­culo
               - Processa volumes
            
            5. **Salvamento**:
               - Salva arquivos Parquet na pasta do ano (ou BUD/)
               - Salva arquivos Excel intermediÃ¡rios (diagnÃ³sticos)
               - Consolida histÃ³rico (concatena, nÃ£o substitui)
            
            6. **Feedback ao usuÃ¡rio**:
               - Barra de progresso mostra status
               - Mensagens de log aparecem em tempo real
               - Mensagem de sucesso ao finalizar
            """)
        
        with st.expander("**7ï¸âƒ£ ConsolidaÃ§Ã£o do HistÃ³rico**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o `salvar_e_consolidar()` ou `salvar_e_consolidar_bud()`
            
            **Processo**:
            1. **Carrega histÃ³rico existente** (se existir):
                    - Tenta carregar: `dados/TC_Ext/historico_consolidado/df_final_historico.parquet`
               - Se nÃ£o existir, cria DataFrame vazio
            
            2. **Adiciona coluna Ano aos dados atuais**:
               - Adiciona coluna `Ano` com valor do ano processado
               - Exemplo: Se processando 2026, todos os registros recebem `Ano = 2026`
            
            3. **Concatena dados**:
               - Concatena dados do ano atual com histÃ³rico existente
               - Usa `pd.concat([historico, dados_atuais])`
            
            4. **Remove duplicatas** (se houver):
               - Verifica e remove registros duplicados
            
            5. **Valida tipos de dados**:
               - Garante que Volume Ã© numÃ©rico (float64)
               - Converte tipos se necessÃ¡rio
            
            6. **Salva histÃ³rico atualizado**:
                    - Salva em: `dados/TC_Ext/historico_consolidado/df_final_historico.parquet`
               - **IMPORTANTE**: HistÃ³rico Ã© sempre **concatenado**, nunca substituÃ­do
            
            **Resultado**: HistÃ³rico contÃ©m dados de todos os anos processados
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 3: Sistema de Busca de Arquivos (Detalhado)
        st.markdown("## ðŸ” SISTEMA DE BUSCA DE ARQUIVOS {#busca-arquivos}")
        
        st.markdown("### LÃ³gica de Busca Detalhada")
        
        st.markdown("""
        O sistema implementa uma **busca hierÃ¡rquica** que prioriza arquivos organizados
        nas pastas do ano, mas permite flexibilidade ao buscar na raiz do projeto quando
        necessÃ¡rio. Isso facilita o trabalho com novos anos sem precisar mover arquivos manualmente.
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **ðŸ“Š Dados REAIS - Ordem de Busca:**
            
            1. `dados/TC_Ext/{ANO}/Dados SAPIENS.xlsx`
            2. `./Dados SAPIENS.xlsx` (raiz)
            
            1. `dados/TC_Ext/{ANO}/Reporting fluxo anexo.xlsx`
            2. `./Reporting fluxo anexo.xlsx` (raiz)
            """)
        
        with col2:
            st.markdown("""
            **ðŸ’° Dados BUDGET - Ordem de Busca:**
            
            1. `dados/TC_Ext/{ANO}/Dados SAPIENS.xlsx`
            2. `./Dados SAPIENS.xlsx` (raiz)
            
            1. `dados/TC_Ext/{ANO}/Reporting fluxo anexo.xlsx`
            2. `./Reporting fluxo anexo.xlsx` (raiz)
            """)
        
        st.markdown("### Exemplos PrÃ¡ticos de Busca")
        
        with st.expander("**Exemplo 1: Processando 2024 (ano existente)**", expanded=False):
            st.markdown("""
            **CenÃ¡rio**: Pasta `dados/TC_Ext/2024/` jÃ¡ existe com arquivos
            
            **Busca de Dados SAPIENS.xlsx**:
            1. âœ… Encontrado em: `dados/TC_Ext/2024/Dados SAPIENS.xlsx`
            2. Sistema usa este arquivo (para na primeira opÃ§Ã£o)
            
            **Resultado**: Arquivo da pasta do ano Ã© usado (prioridade mÃ¡xima)
            """)
        
        with st.expander("**Exemplo 2: Processando 2026 (ano novo)**", expanded=False):
            st.markdown("""
            **CenÃ¡rio**: Pasta `dados/TC_Ext/2026/` nÃ£o existe ainda, arquivo estÃ¡ na raiz
            
            **Busca de Dados SAPIENS.xlsx**:
            1. âŒ NÃ£o encontrado em: `dados/TC_Ext/2026/Dados SAPIENS.xlsx` (pasta nÃ£o existe)
            2. âœ… Encontrado em: `./Dados SAPIENS.xlsx` (raiz do projeto)
            3. Sistema usa arquivo da raiz
            
            **Resultado**: 
            - Sistema cria `dados/TC_Ext/2026/` automaticamente
            - Arquivo da raiz Ã© usado para processamento
            - Arquivos de saÃ­da sÃ£o salvos em `dados/TC_Ext/2026/`
            - **Arquivo da raiz permanece na raiz** (nÃ£o Ã© movido automaticamente)
            """)
        
        with st.expander("**Exemplo 3: Upload de Arquivo para 2026**", expanded=False):
            st.markdown("""
            **CenÃ¡rio**: UsuÃ¡rio faz upload de arquivo para ano 2026
            
            **Processo**:
            1. Sistema cria `dados/TC_Ext/2026/` (se nÃ£o existir)
            2. UsuÃ¡rio faz upload de `Dados SAPIENS.xlsx`
            3. Arquivo Ã© salvo em: `dados/TC_Ext/2026/Dados SAPIENS.xlsx`
            
            **PrÃ³xima busca**:
            1. âœ… Encontrado em: `dados/TC_Ext/2026/Dados SAPIENS.xlsx`
            2. Sistema usa este arquivo (prioridade mÃ¡xima)
            
            **Resultado**: Arquivo uploadado tem prioridade sobre arquivo da raiz
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 4: CriaÃ§Ã£o de Pastas e Estrutura
        st.markdown("## ðŸ“ CRIAÃ‡ÃƒO DE PASTAS E ESTRUTURA {#criacao-pastas}")
        
        st.markdown("### Estrutura Completa de Pastas")
        
        st.code("""
        dados/TC_Ext/
        â”œâ”€â”€ 2024/                    # Ano 2024 (dados REAIS)
        â”‚   â”œâ”€â”€ Dados SAPIENS.xlsx
        â”‚   â”œâ”€â”€ Reporting fluxo anexo.xlsx
        â”‚   â”œâ”€â”€ df_final.parquet
        â”‚   â”œâ”€â”€ df_vol.parquet
        â”‚   â”œâ”€â”€ df_ke5z_group.parquet
        â”‚   â””â”€â”€ BUD/                 # Dados BUDGET do ano 2024
        â”‚       â”œâ”€â”€ Dados SAPIENS.xlsx (opcional)
        â”‚       â”œâ”€â”€ Reporting fluxo anexo.xlsx (opcional)
        â”‚       â”œâ”€â”€ df_final_BUD.parquet
        â”‚       â”œâ”€â”€ df_vol_BUD.parquet
        â”‚       â””â”€â”€ df_ke5z_group_BUD.parquet
        â”‚
        â”œâ”€â”€ 2025/                    # Ano 2025
        â”‚   â””â”€â”€ ...
        â”‚
        â”œâ”€â”€ 2026/                    # Ano 2026 (novo ano)
        â”‚   â””â”€â”€ ...                  # Criado automaticamente
        â”‚
        â””â”€â”€ historico_consolidado/   # HistÃ³rico de todos os anos
            â”œâ”€â”€ df_final_historico.parquet
            â”œâ”€â”€ df_vol_historico.parquet
            â””â”€â”€ BUD/
                â”œâ”€â”€ df_final_historico_BUD.parquet
                â””â”€â”€ df_vol_historico_BUD.parquet
        """, language="text")
        
        st.markdown("### Quando as Pastas SÃ£o Criadas")
        
        with st.expander("**CriaÃ§Ã£o AutomÃ¡tica**", expanded=False):
            st.markdown("""
            **Momento**: Ao iniciar o processamento (funÃ§Ã£o `configurar_ano()`)
            
            **Pastas criadas automaticamente**:
            - `dados/TC_Ext/{ANO}/` - Sempre criada, mesmo que vazia
            - `dados/TC_Ext/{ANO}/BUD/` - Criada apenas para **outputs** do processamento BUDGET
            - `dados/TC_Ext/historico_consolidado/` - Criada se nÃ£o existir
            - `dados/TC_Ext/historico_consolidado/BUD/` - Criada se nÃ£o existir (para BUDGET)
            
            **Comando usado**: `os.makedirs(pasta, exist_ok=True)`
            - `exist_ok=True` significa que nÃ£o dÃ¡ erro se pasta jÃ¡ existe
            - Cria todas as pastas intermediÃ¡rias automaticamente
            """)
        
        with st.expander("**CriaÃ§Ã£o via Upload**", expanded=False):
            st.markdown("""
            **Momento**: Quando usuÃ¡rio faz upload de arquivo
            
            **Processo**:
            1. UsuÃ¡rio seleciona arquivo para upload
            2. Sistema verifica se pasta `dados/TC_Ext/{ANO}/` existe
            3. Se nÃ£o existe: Cria automaticamente com `os.makedirs(pasta_ano, exist_ok=True)`
            4. Salva arquivo em: `dados/TC_Ext/{ANO}/Nome_do_Arquivo.xlsx`
            
            **Resultado**: Pasta do ano Ã© criada antes mesmo do processamento
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 5: Sistema de Upload de Arquivos
        st.markdown("## ðŸ“¤ SISTEMA DE UPLOAD DE ARQUIVOS {#sistema-upload}")
        
        st.markdown("### Funcionalidades do Upload")
        
        st.markdown("""
        O sistema de upload permite que arquivos sejam enviados diretamente pela interface
        web, sem necessidade de colocÃ¡-los manualmente na raiz do projeto ou nas pastas.
        Isso facilita especialmente o trabalho com novos anos ou atualizaÃ§Ãµes de arquivos.
        """)
        
        with st.expander("**Interface de Upload**", expanded=False):
            st.markdown("""
            **LocalizaÃ§Ã£o**: PÃ¡gina "5 - ExtraÃ§Ã£o de Dados" â†’ Aba "ValidaÃ§Ã£o de Arquivos" â†’ SeÃ§Ã£o "ðŸ“¤ Upload de Arquivos"
            
            **Componentes**:
            - Uploaders separados por tipo de processamento (REAIS ou BUDGET)
            - Uploaders separados por arquivo (Dados SAPIENS.xlsx e Reporting fluxo anexo.xlsx)
            - Avisos proativos mostrando se arquivo jÃ¡ existe
            - Mensagens de confirmaÃ§Ã£o apÃ³s upload bem-sucedido
            
            **Layout**: Dois uploaders lado a lado (colunas) para cada tipo de processamento
            """)
        
        with st.expander("**Fluxo Completo de Upload**", expanded=False):
            st.markdown("""
            **Passo 1: VerificaÃ§Ã£o Proativa**
            - Ao carregar a pÃ¡gina, sistema verifica se arquivos jÃ¡ existem
            - Se existem: Mostra aviso âš ï¸ acima do botÃ£o "Browse Files"
            - Aviso informa: "O arquivo jÃ¡ existe e serÃ¡ sobrescrito se vocÃª fizer upload"
            
            **Passo 2: SeleÃ§Ã£o do Arquivo**
            - UsuÃ¡rio clica em "Browse Files"
            - Seleciona arquivo do computador
            - Sistema detecta que arquivo foi selecionado
            
            **Passo 3: VerificaÃ§Ã£o PÃ³s-SeleÃ§Ã£o**
            - Sistema verifica novamente se arquivo existe na pasta de destino
            - Se existe: Mostra aviso adicional e botÃ£o "ðŸ”„ Confirmar Sobrescrita"
            - Se nÃ£o existe: Prossegue para salvamento automÃ¡tico
            
            **Passo 4: ConfirmaÃ§Ã£o (se necessÃ¡rio)**
            - Se arquivo existe, usuÃ¡rio deve clicar em "ðŸ”„ Confirmar Sobrescrita"
            - BotÃ£o sÃ³ aparece se arquivo realmente existe
            - ConfirmaÃ§Ã£o evita sobrescrita acidental
            
            **Passo 5: Salvamento**
            - Arquivo Ã© salvo em: `dados/TC_Ext/{ANO_SELECIONADO}/Nome_do_Arquivo.xlsx`
            - Pasta do ano Ã© criada automaticamente se nÃ£o existir
            - Mensagem de sucesso Ã© exibida
            
            **Passo 6: AtualizaÃ§Ã£o AutomÃ¡tica**
            - PÃ¡gina recarrega automaticamente (`st.rerun()`)
            - Status dos arquivos Ã© atualizado
            - Avisos sÃ£o atualizados (se arquivo agora existe)
            """)
        
        with st.expander("**Vantagens do Sistema de Upload**", expanded=False):
            st.markdown("""
            âœ… **OrganizaÃ§Ã£o AutomÃ¡tica**: Arquivos sÃ£o salvos na pasta correta automaticamente
            
            âœ… **Flexibilidade**: NÃ£o precisa colocar arquivos na raiz do projeto
            
            âœ… **SeguranÃ§a**: Avisos preventivos evitam sobrescrita acidental
            
            âœ… **Facilidade**: Especialmente Ãºtil para novos anos (ex: 2026)
            
            âœ… **Rastreabilidade**: Mensagens claras mostram onde arquivo foi salvo
            
            âœ… **ValidaÃ§Ã£o**: Sistema verifica existÃªncia antes e depois do upload
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 6: Processamento e ExecuÃ§Ã£o
        st.markdown("## âš™ï¸ PROCESSAMENTO E EXECUÃ‡ÃƒO {#processamento-execucao}")
        
        st.markdown("### Fluxo de ExecuÃ§Ã£o Completo")
        
        st.markdown("""
        O processamento segue uma sequÃªncia bem definida, garantindo que todos os passos
        sejam executados na ordem correta e que os dados sejam processados e salvos adequadamente.
        """)
        
        with st.expander("**Fase 1: PreparaÃ§Ã£o**", expanded=False):
            st.markdown("""
            1. **ConfiguraÃ§Ã£o do Ano**:
               - Chama `configurar_ano()` ou `configurar_ano_bud()`
               - Cria estrutura de pastas
               - Define caminhos de entrada e saÃ­da
            
            2. **Busca de Arquivos**:
               - Busca `Dados SAPIENS.xlsx` na ordem de prioridade
               - Busca `Reporting fluxo anexo.xlsx` na ordem de prioridade
               - Valida se arquivos foram encontrados
            
            3. **ValidaÃ§Ã£o**:
               - Verifica se todos os arquivos necessÃ¡rios existem
               - Se faltar arquivo: Retorna erro ou aviso (dependendo da configuraÃ§Ã£o)
            """)
        
        with st.expander("**Fase 2: Leitura e TransformaÃ§Ã£o**", expanded=False):
            st.markdown("""
            1. **Leitura dos Dados Principais**:
               - LÃª guia "Sapiens" ou "Voz de custo BDG" do Reporting fluxo anexo.xlsx
               - Cria DataFrame inicial (`df_KE5Z`)
            
            2. **Merge com Base Conso**:
               - LÃª guia "Base conso" do Dados SAPIENS.xlsx
               - Faz merge adicionando coluna `Custo` (VariÃ¡vel/Fixo)
            
            3. **Processamento de Rateio**:
               - LÃª guia "Rateio" ou "Rateio BDG"
               - Transforma colunas de meses em linhas
               - Cria DataFrame com percentuais de rateio por veÃ­culo
            
            4. **Merge e CÃ¡lculo por VeÃ­culo**:
               - Merge com dados principais
               - Calcula valores por veÃ­culo (CC21, CC22, etc.)
            
            5. **Processamento de Volume**:
               - LÃª guia "Volume" ou "Volume BDG"
               - Transforma colunas de meses em linhas
               - Cria DataFrame com volumes
            
            6. **Merge Final com Volume**:
               - Adiciona coluna Volume ao DataFrame principal
            """)
        
        with st.expander("**Fase 3: Salvamento e ConsolidaÃ§Ã£o**", expanded=False):
            st.markdown("""
            1. **Salvamento na Pasta do Ano**:
               - Salva `df_final.parquet` em `dados/TC_Ext/{ANO}/` (ou `BUD/`)
               - Salva `df_vol.parquet`
               - Salva `df_ke5z_group.parquet`
               - Salva arquivos Excel intermediÃ¡rios (diagnÃ³sticos)
            
            2. **ConsolidaÃ§Ã£o do HistÃ³rico**:
               - Carrega histÃ³rico existente (se houver)
               - Adiciona coluna `Ano` aos dados atuais
               - Concatena dados atuais com histÃ³rico
               - Remove duplicatas
               - Salva histÃ³rico atualizado
            
            3. **ValidaÃ§Ã£o Final**:
               - Verifica tipos de dados
               - Valida integridade dos arquivos salvos
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 7: CenÃ¡rios de Uso
        st.markdown("## ðŸ“‹ CENÃRIOS DE USO {#cenarios-uso}")
        
        st.markdown("### Casos PrÃ¡ticos Completos")
        
        with st.expander("**CenÃ¡rio 1: Primeira Vez Processando um Novo Ano (ex: 2026)**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: Nunca processou dados de 2026, arquivos estÃ£o na raiz do projeto
            
            **Passo a Passo**:
            
            1. **Acessar pÃ¡gina de extraÃ§Ã£o**:
               - Selecionar ano: 2026
               - Selecionar tipo: "ðŸ“Š Dados REAIS" ou "ðŸ”„ Ambos"
            
            2. **OpÃ§Ã£o A - Usar Upload** (Recomendado):
               - Ir para aba "ValidaÃ§Ã£o de Arquivos"
                    - Fazer upload de `Dados SAPIENS.xlsx` â†’ Salvo em `dados/TC_Ext/2026/`
                    - Fazer upload de `Reporting fluxo anexo.xlsx` â†’ Salvo em `dados/TC_Ext/2026/`
            
            3. **OpÃ§Ã£o B - Usar Arquivos da Raiz**:
               - Colocar arquivos na raiz do projeto
               - Sistema buscarÃ¡ automaticamente na raiz se nÃ£o encontrar na pasta do ano
            
            4. **Executar processamento**:
               - Clicar em "ðŸš€ Executar tc_ext/notebooks/dados.ipynb"
                    - Sistema cria `dados/TC_Ext/2026/` automaticamente
               - Sistema busca arquivos (encontra na raiz ou na pasta do ano)
                    - Processa e salva em `dados/TC_Ext/2026/`
               - Consolida histÃ³rico
            
            **Resultado**: 
                - Pasta `dados/TC_Ext/2026/` criada com arquivos processados
            - HistÃ³rico atualizado com dados de 2026
            """)
        
        with st.expander("**CenÃ¡rio 2: Atualizar Arquivos de um Ano Existente**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: JÃ¡ processou 2024 antes, mas recebeu arquivos atualizados
            
            **Passo a Passo**:
            
            1. **Acessar pÃ¡gina de extraÃ§Ã£o**:
               - Selecionar ano: 2024
               - Selecionar tipo: "ðŸ“Š Dados REAIS"
            
            2. **Verificar arquivos existentes**:
               - Sistema mostra aviso: "âš ï¸ O arquivo jÃ¡ existe"
               - Aviso aparece antes mesmo de fazer upload
            
            3. **Fazer upload do arquivo atualizado**:
               - Selecionar arquivo atualizado
               - Sistema mostra aviso: "Arquivo serÃ¡ sobrescrito"
               - Clicar em "ðŸ”„ Confirmar Sobrescrita"
               - Arquivo Ã© salvo substituindo o anterior
            
            4. **Executar processamento**:
               - Clicar em "ðŸš€ Executar tc_ext/notebooks/dados.ipynb"
                    - Sistema usa arquivo atualizado de `dados/TC_Ext/2024/`
               - Processa e atualiza arquivos Parquet
               - Atualiza histÃ³rico (concatena, nÃ£o substitui)
            
            **Resultado**: 
            - Arquivos de 2024 atualizados
            - HistÃ³rico contÃ©m versÃ£o mais recente
            """)
        
        with st.expander("**CenÃ¡rio 3: Processar Ambos (REAIS e BUDGET) para Novo Ano**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: Processar dados REAIS e BUDGET de 2026 pela primeira vez
            
            **Passo a Passo**:
            
            1. **Preparar arquivos REAIS**:
                    - Upload de `Dados SAPIENS.xlsx` (REAIS) â†’ `dados/TC_Ext/2026/`
                    - Upload de `Reporting fluxo anexo.xlsx` (REAIS) â†’ `dados/TC_Ext/2026/`
            
            2. **Preparar arquivos BUDGET** (se diferentes):
                    - Upload de `Dados SAPIENS.xlsx` (BUD) â†’ `dados/TC_Ext/2026/` (mesmo arquivo ou versÃ£o BUD)
                    - Upload de `Reporting fluxo anexo.xlsx` (BUD) â†’ `dados/TC_Ext/2026/` (com guias BDG)
            
            3. **Executar processamento**:
               - Selecionar tipo: "ðŸ”„ Ambos"
               - Clicar em "ðŸš€ Executar Ambos"
                    - Sistema processa REAIS primeiro â†’ Salva em `dados/TC_Ext/2026/`
                    - Sistema processa BUDGET depois â†’ Salva em `dados/TC_Ext/2026/BUD/`
               - Consolida ambos os histÃ³ricos
            
            **Resultado**: 
                - Estrutura completa criada: `dados/TC_Ext/2026/` e `dados/TC_Ext/2026/BUD/`
            - HistÃ³ricos REAIS e BUDGET atualizados
            """)
        
        with st.expander("**CenÃ¡rio 4: Processar Apenas BUDGET para Ano Existente**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: JÃ¡ processou REAIS de 2024, agora quer processar BUDGET
            
            **Passo a Passo**:
            
            1. **Preparar arquivos BUDGET**:
                    - Upload de `Dados SAPIENS.xlsx` (BUD) â†’ `dados/TC_Ext/2024/`
                    - Upload de `Reporting fluxo anexo.xlsx` (BUD) â†’ `dados/TC_Ext/2024/`
            
            2. **Executar processamento BUDGET**:
               - Selecionar tipo: "ðŸ’° Dados BUDGET"
               - Clicar em "ðŸš€ Executar tc_ext/notebooks/dados_BUD.ipynb"
                    - Sistema cria `dados/TC_Ext/2024/BUD/` automaticamente
                    - Processa e salva em `dados/TC_Ext/2024/BUD/`
               - Consolida histÃ³rico BUDGET
            
            **Resultado**: 
                - Pasta `dados/TC_Ext/2024/BUD/` criada com dados de Budget
            - HistÃ³rico BUDGET atualizado
            - Dados REAIS permanecem inalterados
            """)
        
        st.markdown("---")
        
        st.success("""
        **âœ… Este capÃ­tulo descreve completamente o funcionamento do sistema de atualizaÃ§Ã£o**
        e extraÃ§Ã£o de dados. Use estas informaÃ§Ãµes para realizar atualizaÃ§Ãµes de forma
        segura e eficiente, especialmente ao trabalhar com novos anos ou atualizar
        arquivos existentes.
        """)


# ==========================================
# SEÃ‡ÃƒO 5: GUIA DE BEST ESTIMATE
# ==========================================
elif indice_selecionado == "ðŸ”® Guia de Best Estimate":
    st.header("ðŸ”® Guia de Best Estimate â€” TC Ext + TC VeÃ­culos")

    with st.expander("ðŸš— **TC VeÃ­culos â€” Resumo operacional (Simulador + consumo na Home)**", expanded=False):
        st.markdown("""
        ### ðŸ”® O que Ã© o Best Estimate (TC VeÃ­culos)

        O Best Estimate (BE) projeta custos futuros a partir da mÃ©dia histÃ³rica jÃ¡ realizada,
        ajustada por premissas de **sensibilidade**, **inflaÃ§Ã£o** e **volume**.

        **Onde configurar e gerar o Forecast:**
        - PÃ¡gina Streamlit: `pages/2 - Best Estimate - Simulador.py`
        - LÃ³gica principal: `tc_principal/pages/best_estimate_simulador_tc.py`

        **Arquivos gerados:**
        - `dados/TC_Principal/Forecast/forecast_completo.parquet` (coluna `Tipo = 'BE'`)
        - `dados/TC_Principal/Forecast/premissas.json`

        **Onde o Forecast Ã© consumido/analisado:**
        - `tc_principal/pages/home_tc.py` (tabs) â€” compara Real vs BE no layout da Home

        **Pontos de atenÃ§Ã£o (operacional):**
        - Se o Forecast parecer "nÃ£o atualizar", confirme que o `forecast_completo.parquet` foi regravado.
        - Se a granularidade por veÃ­culo depender de rateio, a funÃ§Ã£o `ratear_be_por_veiculo()` (em `tc_principal/shared.py`)
          Ã© aplicada nos fluxos que exigem visÃ£o por veÃ­culo.
        """)
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
    <h2 style="color: white; margin: 0;">ðŸ”® DocumentaÃ§Ã£o Completa do Best Estimate</h2>
    <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            Teoria, CÃ¡lculos, Estrutura e Funcionamento do Sistema de PrevisÃ£o
    </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Ãndice interno
    st.markdown("## ðŸ“‹ Ãndice do Guia")
    st.markdown("""
    ### ðŸ“– CapÃ­tulo 1: Teoria e Funcionamento do Best Estimate
    1. [O que Ã© Best Estimate?](#o-que-e-best-estimate)
    2. [Teoria e Conceitos Fundamentais](#teoria-conceitos)
    3. [CÃ¡lculo de MÃ©dias HistÃ³ricas](#calculo-medias)
    4. [Sensibilidade e InflaÃ§Ã£o](#sensibilidade-inflacao)
    5. [FÃ³rmulas e LÃ³gica de CÃ¡lculo](#formulas-logica)
    6. [Tipos de Custos: Fixo vs VariÃ¡vel](#tipos-custos)
    7. [Volume e ProporÃ§Ãµes](#volume-proporcoes)
    
    ### ðŸ”„ CapÃ­tulo 2: Estrutura, AtualizaÃ§Ã£o e PÃ¡ginas
    1. [Estrutura de Pastas do Forecast](#estrutura-forecast)
    2. [Ordem CronolÃ³gica de AtualizaÃ§Ã£o](#ordem-cronologica-forecast)
    3. [PÃ¡gina 2 - Best Estimate Simulador](#pagina-simulador)
    4. [PÃ¡gina - Best Estimate (AnÃ¡lise)](#pagina-analise)
    5. [Fluxo de Dados e Processamento](#fluxo-dados-forecast)
    6. [Arquivos Gerados](#arquivos-gerados-forecast)
    7. [CenÃ¡rios de Uso](#cenarios-uso-forecast)
    """)
    
    st.markdown("---")
    
    # ==========================================
    # CAPÃTULO 1: TEORIA E FUNCIONAMENTO DO BEST ESTIMATE
    # ==========================================
    
    with st.expander("ðŸ“– **CapÃ­tulo 1: Teoria e Funcionamento do Best Estimate**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">ðŸ“– CapÃ­tulo 1: Teoria e Funcionamento do Best Estimate</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Conceitos, Teoria e CÃ¡lculos do Sistema de PrevisÃ£o de Custos
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # SeÃ§Ã£o 1: O que Ã© Best Estimate?
        st.markdown("## ðŸŽ¯ O QUE Ã‰ BEST ESTIMATE? {#o-que-e-best-estimate}")
        
        st.markdown("""
        ### DefiniÃ§Ã£o e Conceito
        
        **Best Estimate** (Melhor Estimativa) Ã© uma metodologia de previsÃ£o de custos que combina:
        - **Dados histÃ³ricos** (mÃ©dias de perÃ­odos anteriores)
        - **Ajustes por sensibilidade** (resposta a variaÃ§Ãµes de volume)
        - **Ajustes por inflaÃ§Ã£o** (correÃ§Ã£o monetÃ¡ria)
        - **ClassificaÃ§Ã£o de custos** (Fixo vs VariÃ¡vel)
        
        **Objetivo Principal:**
        Prever os custos futuros com base em padrÃµes histÃ³ricos, ajustados para refletir mudanÃ§as esperadas
        em volume de produÃ§Ã£o e inflaÃ§Ã£o, permitindo planejamento financeiro mais preciso.
        
        **AplicaÃ§Ã£o no SCI:**
        O Best Estimate Ã© usado para gerar previsÃµes de custos para perÃ­odos futuros, permitindo comparaÃ§Ãµes
        entre o que foi planejado (Budget), o que realmente aconteceu (Real) e o que se espera que aconteÃ§a
        (Best Estimate/Forecast).
        """)
        
        st.info("""
        **ðŸ’¡ Importante**: Best Estimate nÃ£o Ã© uma simples projeÃ§Ã£o linear. Ele considera a natureza dos custos
        (fixos ou variÃ¡veis) e aplica sensibilidades diferentes para cada tipo, resultando em previsÃµes mais
        realistas e acuradas.
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 2: Teoria e Conceitos Fundamentais
        st.markdown("## ðŸ“š TEORIA E CONCEITOS FUNDAMENTAIS {#teoria-conceitos}")
        
        st.markdown("""
        ### Fundamentos TeÃ³ricos
        
        **1. PrincÃ­pio da MÃ©dia HistÃ³rica:**
        - O Best Estimate parte do pressuposto de que o comportamento histÃ³rico Ã© um bom indicador do futuro
        - MÃ©dias calculadas sobre perÃ­odos selecionados fornecem uma base sÃ³lida para previsÃµes
        - PerÃ­odos anÃ´malos podem ser excluÃ­dos para melhorar a acurÃ¡cia
        
        **2. PrincÃ­pio da Sensibilidade:**
        - Custos **fixos** nÃ£o variam com volume (sensibilidade = 0%)
        - Custos **variÃ¡veis** variam proporcionalmente ao volume (sensibilidade = 100%)
        - Sensibilidades intermediÃ¡rias (0% < sensibilidade < 100%) representam custos semi-variÃ¡veis
        
        **3. PrincÃ­pio da InflaÃ§Ã£o:**
        - InflaÃ§Ã£o afeta todos os custos de forma uniforme
        - Ã‰ aplicada como um fator multiplicador sobre o custo ajustado por sensibilidade
        - Permite correÃ§Ã£o monetÃ¡ria para perÃ­odos futuros
        
        **4. PrincÃ­pio da Proporcionalidade de Volume:**
        - A variaÃ§Ã£o de volume impacta diferentemente custos fixos e variÃ¡veis
        - Custos fixos sÃ£o "diluÃ­dos" quando o volume aumenta (CPU diminui)
        - Custos variÃ¡veis aumentam proporcionalmente ao volume
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 3: CÃ¡lculo de MÃ©dias HistÃ³ricas
        st.markdown("## ðŸ“Š CÃLCULO DE MÃ‰DIAS HISTÃ“RICAS {#calculo-medias}")
        
        st.markdown("""
        ### Processo de CÃ¡lculo de MÃ©dias
        
        **Passo 1: SeleÃ§Ã£o de PerÃ­odos**
        - O usuÃ¡rio seleciona quais perÃ­odos histÃ³ricos serÃ£o usados para calcular a mÃ©dia
        - Exemplo: Janeiro 2024, Fevereiro 2024, MarÃ§o 2024
        - PerÃ­odos podem ser excluÃ­dos se forem considerados anÃ´malos
        
        **Passo 2: Filtragem de Dados**
        - Aplicam-se os mesmos filtros usados na anÃ¡lise (Oficina, VeÃ­culo, Type 05, Type 06, etc.)
        - Garante que a mÃ©dia seja calculada sobre o mesmo contexto operacional
        
        **Passo 3: Agrupamento e AgregaÃ§Ã£o**
        - Dados sÃ£o agrupados por chaves Ãºnicas: `Oficina`, `VeÃ­culo`, `Tipo_Custo`, `Type 06`, etc.
        - Para cada grupo, calcula-se a mÃ©dia dos valores histÃ³ricos
        - FÃ³rmula: `MÃ©dia_HistÃ³rica = Î£(Valores_HistÃ³ricos) / NÃºmero_de_PerÃ­odos`
        
        **Passo 4: Volume MÃ©dio HistÃ³rico**
        - Calcula-se tambÃ©m o volume mÃ©dio histÃ³rico para os mesmos perÃ­odos
        - Usado para calcular proporÃ§Ãµes de volume futuro vs histÃ³rico
        - FÃ³rmula: `Volume_MÃ©dio_HistÃ³rico = Î£(Volumes_HistÃ³ricos) / NÃºmero_de_PerÃ­odos`
        
        **Exemplo PrÃ¡tico:**
        ```
        PerÃ­odos selecionados: Janeiro 2024, Fevereiro 2024, MarÃ§o 2024
        
        Para Oficina A, VeÃ­culo CC21, Type 06 "Material":
        - Janeiro 2024: R$ 10.000
        - Fevereiro 2024: R$ 12.000
        - MarÃ§o 2024: R$ 11.000
        
        MÃ©dia HistÃ³rica = (10.000 + 12.000 + 11.000) / 3 = R$ 11.000
        ```
        """)
        
        with st.expander("**ðŸ” Detalhes TÃ©cnicos do CÃ¡lculo de MÃ©dias**", expanded=False):
            st.markdown("""
            **Agrupamento por Chaves:**
            - O sistema agrupa dados por mÃºltiplas dimensÃµes simultaneamente
            - Chaves padrÃ£o: `['Oficina', 'VeÃ­culo', 'Tipo_Custo', 'Type 06', ...]`
            - Cada combinaÃ§Ã£o Ãºnica de chaves gera uma linha no forecast
            
            **Tratamento de Dados Faltantes:**
            - Se um perÃ­odo nÃ£o tiver dados para uma combinaÃ§Ã£o de chaves, ele Ã© excluÃ­do do cÃ¡lculo
            - A mÃ©dia Ã© calculada apenas sobre perÃ­odos com dados disponÃ­veis
            - Isso evita distorÃ§Ãµes por perÃ­odos incompletos
            
            **NormalizaÃ§Ã£o de PerÃ­odos:**
            - PerÃ­odos sÃ£o normalizados para comparaÃ§Ã£o (ex: "Janeiro 2024" â†’ "janeiro 2024")
            - Permite comparaÃ§Ã£o case-insensitive e tolerante a espaÃ§os
            - Anos sÃ£o extraÃ­dos dos perÃ­odos para filtragem adicional
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 4: Sensibilidade e InflaÃ§Ã£o
        st.markdown("## âš™ï¸ SENSIBILIDADE E INFLAÃ‡ÃƒO {#sensibilidade-inflacao}")
        
        st.markdown("""
        ### Sensibilidade ao Volume
        
        **Conceito:**
        Sensibilidade mede o quanto um custo responde a variaÃ§Ãµes no volume de produÃ§Ã£o.
        
        **Tipos de Sensibilidade:**
        
        **1. Sensibilidade Fixa (0%):**
        - Aplicada a custos **fixos**
        - Independente da variaÃ§Ã£o de volume, o custo permanece constante
        - Exemplos: Aluguel, salÃ¡rios fixos, depreciaÃ§Ã£o
        - FÃ³rmula: `Custo_Ajustado = Custo_Original` (sem alteraÃ§Ã£o)
        
        **2. Sensibilidade VariÃ¡vel (100%):**
        - Aplicada a custos **variÃ¡veis**
        - Varia proporcionalmente ao volume
        - Se volume aumenta 10%, custo aumenta 10%
        - Exemplos: MatÃ©ria-prima, energia variÃ¡vel, comissÃµes
        - FÃ³rmula: `Custo_Ajustado = Custo_Original * (Volume_Novo / Volume_HistÃ³rico)`
        
        **3. Sensibilidades IntermediÃ¡rias (0% < sensibilidade < 100%):**
        - Aplicadas a custos **semi-variÃ¡veis**
        - Resposta parcial a variaÃ§Ãµes de volume
        - Exemplo: Se sensibilidade = 50% e volume aumenta 10%, custo aumenta 5%
        - FÃ³rmula: `Custo_Ajustado = Custo_Original * (1 + (VariaÃ§Ã£o_Volume * Sensibilidade))`
        
        **4. Sensibilidade por Type 06:**
        - Cada Type 06 pode ter sua prÃ³pria sensibilidade especÃ­fica
        - Permite ajustes finos por categoria de custo
        - Sobrescreve a sensibilidade geral (Fixo/VariÃ¡vel) quando configurada
        """)
        
        st.markdown("""
        ### InflaÃ§Ã£o
        
        **Conceito:**
        InflaÃ§Ã£o Ã© aplicada como um ajuste monetÃ¡rio uniforme sobre todos os custos, independente
        de serem fixos ou variÃ¡veis.
        
        **AplicaÃ§Ã£o:**
        - InflaÃ§Ã£o Ã© configurada como percentual (ex: 5% ao ano)
        - Ã‰ aplicada apÃ³s o ajuste por sensibilidade
        - FÃ³rmula: `Custo_Final = Custo_Ajustado_Sensibilidade * (1 + InflaÃ§Ã£o/100)`
        
        **Exemplo:**
        ```
        Custo mÃ©dio histÃ³rico: R$ 10.000
        VariaÃ§Ã£o de volume: +10%
        Sensibilidade: 50%
        InflaÃ§Ã£o: 5%
        
        Passo 1: Ajuste por sensibilidade
        VariaÃ§Ã£o_ajustada = 10% * 50% = 5%
        Custo_ajustado = 10.000 * (1 + 0.05) = R$ 10.500
        
        Passo 2: Aplicar inflaÃ§Ã£o
        Custo_final = 10.500 * (1 + 0.05) = R$ 11.025
        ```
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 5: FÃ³rmulas e LÃ³gica de CÃ¡lculo
        st.markdown("## ðŸ§® FÃ“RMULAS E LÃ“GICA DE CÃLCULO {#formulas-logica}")
        
        st.markdown("""
        ### FÃ³rmula Completa do Best Estimate
        
        **FÃ³rmula Geral (linha a linha):**
        ```
        Best_Estimate = MÃ©dia_HistÃ³rica * Fator_VariaÃ§Ã£o * Fator_InflaÃ§Ã£o
        ```
        
        **Onde:**
        - `MÃ©dia_HistÃ³rica` = MÃ©dia dos custos histÃ³ricos para a combinaÃ§Ã£o de chaves
        - `Fator_VariaÃ§Ã£o` = 1 + (VariaÃ§Ã£o_Percentual_Volume * Sensibilidade)
        - `Fator_InflaÃ§Ã£o` = 1 + (InflaÃ§Ã£o / 100)
        
        **CÃ¡lculo Detalhado Passo a Passo:**
        
        **1. Calcular ProporÃ§Ã£o de Volume:**
        ```
        proporÃ§Ã£o_volume = Volume_do_MÃªs_Futuro / Volume_MÃ©dio_HistÃ³rico
        ```
        
        **2. Calcular VariaÃ§Ã£o Percentual:**
        ```
        variaÃ§Ã£o_percentual = proporÃ§Ã£o_volume - 1.0
        ```
        - Se `variaÃ§Ã£o_percentual > 0`: Volume aumentou
        - Se `variaÃ§Ã£o_percentual < 0`: Volume diminuiu
        - Se `variaÃ§Ã£o_percentual = 0`: Volume permaneceu igual
        
        **3. Aplicar Sensibilidade:**
        ```
        variaÃ§Ã£o_ajustada = variaÃ§Ã£o_percentual * sensibilidade
        ```
        - Para custos fixos: `sensibilidade = 0` â†’ `variaÃ§Ã£o_ajustada = 0`
        - Para custos variÃ¡veis: `sensibilidade = 1.0` â†’ `variaÃ§Ã£o_ajustada = variaÃ§Ã£o_percentual`
        
        **4. Calcular Fator de VariaÃ§Ã£o:**
        ```
        fator_variaÃ§Ã£o = 1.0 + variaÃ§Ã£o_ajustada
        ```
        
        **5. Calcular Fator de InflaÃ§Ã£o:**
        ```
        fator_inflaÃ§Ã£o = 1.0 + (inflaÃ§Ã£o / 100.0)
        ```
        
        **6. Calcular Best Estimate Final:**
        ```
        Best_Estimate = MÃ©dia_HistÃ³rica * fator_variaÃ§Ã£o * fator_inflaÃ§Ã£o
        ```
        """)
        
        with st.expander("**ðŸ“ Exemplo Completo de CÃ¡lculo**", expanded=False):
            st.markdown("""
            **CenÃ¡rio:**
            - MÃ©dia histÃ³rica: R$ 10.000
            - Volume mÃ©dio histÃ³rico: 1.000 unidades
            - Volume do mÃªs futuro: 1.100 unidades
            - Tipo de custo: VariÃ¡vel (sensibilidade = 100%)
            - InflaÃ§Ã£o: 5%
            
            **CÃ¡lculo:**
            
            **Passo 1:** ProporÃ§Ã£o de volume
            ```
            proporÃ§Ã£o = 1.100 / 1.000 = 1.1
            ```
            
            **Passo 2:** VariaÃ§Ã£o percentual
            ```
            variaÃ§Ã£o = 1.1 - 1.0 = 0.1 (10% de aumento)
            ```
            
            **Passo 3:** Aplicar sensibilidade
            ```
            variaÃ§Ã£o_ajustada = 0.1 * 1.0 = 0.1 (10%)
            ```
            
            **Passo 4:** Fator de variaÃ§Ã£o
            ```
            fator_variaÃ§Ã£o = 1.0 + 0.1 = 1.1
            ```
            
            **Passo 5:** Fator de inflaÃ§Ã£o
            ```
            fator_inflaÃ§Ã£o = 1.0 + (5/100) = 1.05
            ```
            
            **Passo 6:** Best Estimate
            ```
            Best_Estimate = 10.000 * 1.1 * 1.05 = R$ 11.550
            ```
            
            **InterpretaÃ§Ã£o:**
            O custo previsto Ã© R$ 11.550, representando:
            - Aumento de 10% devido ao aumento de volume (de 1.000 para 1.100 unidades)
            - Aumento adicional de 5% devido Ã  inflaÃ§Ã£o
            - Total: 15.5% de aumento sobre a mÃ©dia histÃ³rica
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 6: Tipos de Custos
        st.markdown("## ðŸ’° TIPOS DE CUSTOS: FIXO VS VARIÃVEL {#tipos-custos}")
        
        st.markdown("""
        ### ClassificaÃ§Ã£o de Custos
        
        **Custos Fixos:**
        - **CaracterÃ­sticas:** NÃ£o variam com o volume de produÃ§Ã£o
        - **Sensibilidade:** 0% (zero por cento)
        - **Exemplos:** Aluguel, salÃ¡rios fixos, depreciaÃ§Ã£o, seguros
        - **Comportamento no Best Estimate:**
          - MÃ©dia histÃ³rica Ã© mantida (sem ajuste por volume)
          - Apenas inflaÃ§Ã£o Ã© aplicada
          - FÃ³rmula: `Best_Estimate_Fixo = MÃ©dia_HistÃ³rica_Fixo * (1 + InflaÃ§Ã£o/100)`
        
        **Custos VariÃ¡veis:**
        - **CaracterÃ­sticas:** Variam proporcionalmente ao volume de produÃ§Ã£o
        - **Sensibilidade:** 100% (cem por cento)
        - **Exemplos:** MatÃ©ria-prima, energia variÃ¡vel, comissÃµes, peÃ§as de reposiÃ§Ã£o
        - **Comportamento no Best Estimate:**
          - MÃ©dia histÃ³rica Ã© ajustada pela proporÃ§Ã£o de volume
          - InflaÃ§Ã£o Ã© aplicada sobre o valor ajustado
          - FÃ³rmula: `Best_Estimate_VariÃ¡vel = MÃ©dia_HistÃ³rica_VariÃ¡vel * (Volume_Futuro/Volume_HistÃ³rico) * (1 + InflaÃ§Ã£o/100)`
        
        **IdentificaÃ§Ã£o no Sistema:**
        - A coluna `Custo` (ou `Tipo_Custo`) contÃ©m os valores: `'Fixo'` ou `'VariÃ¡vel'`
        - Esta classificaÃ§Ã£o vem do merge com a Base Conso (Dados SAPIENS.xlsx)
        - Cada linha de dados deve ter esta classificaÃ§Ã£o para o cÃ¡lculo correto
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 7: Volume e ProporÃ§Ãµes
        st.markdown("## ðŸ“ˆ VOLUME E PROPORÃ‡Ã•ES {#volume-proporcoes}")
        
        st.markdown("""
        ### ImportÃ¢ncia do Volume no Best Estimate
        
        **Volume como Base de CÃ¡lculo:**
        - O volume futuro Ã© usado para calcular a proporÃ§Ã£o em relaÃ§Ã£o ao volume histÃ³rico
        - Esta proporÃ§Ã£o determina o ajuste aplicado aos custos variÃ¡veis
        - Volume mÃ©dio histÃ³rico Ã© calculado sobre os mesmos perÃ­odos usados para a mÃ©dia de custos
        
        **CÃ¡lculo de ProporÃ§Ã£o:**
        ```
        proporÃ§Ã£o = Volume_MÃªs_Futuro / Volume_MÃ©dio_HistÃ³rico
        ```
        
        **InterpretaÃ§Ã£o da ProporÃ§Ã£o:**
        - `proporÃ§Ã£o > 1.0`: Volume futuro Ã© maior que o histÃ³rico â†’ Custos variÃ¡veis aumentam
        - `proporÃ§Ã£o < 1.0`: Volume futuro Ã© menor que o histÃ³rico â†’ Custos variÃ¡veis diminuem
        - `proporÃ§Ã£o = 1.0`: Volume futuro igual ao histÃ³rico â†’ Sem ajuste por volume (apenas inflaÃ§Ã£o)
        
        **Impacto nos Custos:**
        - **Custos Fixos:** NÃ£o sÃ£o afetados pela proporÃ§Ã£o (sensibilidade = 0%)
        - **Custos VariÃ¡veis:** SÃ£o multiplicados pela proporÃ§Ã£o (sensibilidade = 100%)
        - **Custos Semi-VariÃ¡veis:** SÃ£o multiplicados por `1 + (proporÃ§Ã£o - 1) * sensibilidade`
        """)
        
        st.success("""
        **âœ… Este capÃ­tulo descreve completamente a teoria e funcionamento do Best Estimate.**
        Use estas informaÃ§Ãµes para entender como as previsÃµes sÃ£o calculadas e como os parÃ¢metros
        (sensibilidade, inflaÃ§Ã£o, perÃ­odos histÃ³ricos) impactam os resultados.
        """)
    
    # ==========================================
    # CAPÃTULO 2: ESTRUTURA, ATUALIZAÃ‡ÃƒO E PÃGINAS
    # ==========================================
    
    with st.expander("ðŸ”„ **CapÃ­tulo 2: Estrutura, AtualizaÃ§Ã£o e PÃ¡ginas**", expanded=False):
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; margin: 2rem 0; color: white;">
            <h2 style="color: white; margin: 0;">ðŸ”„ CapÃ­tulo 2: Estrutura, AtualizaÃ§Ã£o e PÃ¡ginas</h2>
            <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
                Estrutura de Pastas, Ordem de AtualizaÃ§Ã£o e Funcionalidades das PÃ¡ginas
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # SeÃ§Ã£o 1: Estrutura de Pastas do Forecast
        st.markdown("## ðŸ“ ESTRUTURA DE PASTAS DO FORECAST {#estrutura-forecast}")
        
        st.markdown("""
        ### OrganizaÃ§Ã£o da Pasta `dados/TC_Ext/Forecast/`
        
        A pasta `Forecast/` Ã© criada automaticamente quando o Best Estimate Ã© gerado e contÃ©m
        os arquivos de previsÃ£o calculados pelo sistema.
        
        **Estrutura Completa:**
        ```
        dados/TC_Ext/
        â””â”€â”€ Forecast/                       # ðŸ”® Dados de Best Estimate/Forecast
            â”œâ”€â”€ forecast_completo.parquet   # Forecast completo com todas as linhas
            â”œâ”€â”€ forecast_historico.parquet  # HistÃ³rico de forecasts gerados
            â”œâ”€â”€ forecast_previsao.parquet   # PrevisÃµes futuras
            â”œâ”€â”€ df_final_historico_forecast.parquet  # Dados histÃ³ricos filtrados para forecast
            â””â”€â”€ df_vol_historico.parquet    # Volumes histÃ³ricos para cÃ¡lculo
        ```
        
        **CaracterÃ­sticas:**
        - **CriaÃ§Ã£o AutomÃ¡tica:** A pasta Ã© criada automaticamente se nÃ£o existir
        - **SubstituiÃ§Ã£o:** Arquivos sÃ£o substituÃ­dos a cada geraÃ§Ã£o (nÃ£o concatenados)
        - **Prioridade:** Sistema busca arquivos nesta pasta primeiro antes de usar histÃ³rico consolidado
        - **Formato:** Todos os arquivos sÃ£o Parquet para performance otimizada
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 2: Ordem CronolÃ³gica de AtualizaÃ§Ã£o
        st.markdown("## â±ï¸ ORDEM CRONOLÃ“GICA DE ATUALIZAÃ‡ÃƒO {#ordem-cronologica-forecast}")
        
        st.markdown("### SequÃªncia Completa do Processo")
        
        with st.expander("**1ï¸âƒ£ ConfiguraÃ§Ã£o de ParÃ¢metros**", expanded=False):
            st.markdown("""
            **Onde**: PÃ¡gina 2 (Simulador) ou PÃ¡gina 3 (AnÃ¡lise)
            
            **Processo**:
            1. UsuÃ¡rio seleciona **perÃ­odos histÃ³ricos** para calcular a mÃ©dia
               - Exemplo: Janeiro 2024, Fevereiro 2024, MarÃ§o 2024
               - PerÃ­odos podem ser excluÃ­dos se anÃ´malos
            
            2. UsuÃ¡rio configura **sensibilidades**:
               - Sensibilidade para custos fixos (geralmente 0%)
               - Sensibilidade para custos variÃ¡veis (geralmente 100%)
               - Sensibilidades especÃ­ficas por Type 06 (opcional)
            
            3. UsuÃ¡rio configura **inflaÃ§Ã£o**:
               - Percentual de inflaÃ§Ã£o anual (ex: 5%)
               - Pode ser aplicada globalmente ou por Type 06
            
            4. UsuÃ¡rio seleciona **perÃ­odos futuros** para forecast:
               - Exemplo: Abril 2024, Maio 2024, Junho 2024
               - Volumes futuros sÃ£o informados ou calculados
            
            **Resultado**: Sistema tem todos os parÃ¢metros necessÃ¡rios para calcular o forecast
            """)
        
        with st.expander("**2ï¸âƒ£ Carregamento de Dados HistÃ³ricos**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o `load_data()` nas pÃ¡ginas 2 e 3
            
            **Ordem de Prioridade de Busca**:
                1. **Primeira opÃ§Ã£o**: `dados/TC_Ext/Forecast/forecast_completo.parquet`
               - Se existir, pode ser usado como base (mas forecast Ã© recalculado)
            
                2. **Segunda opÃ§Ã£o**: `dados/TC_Ext/historico_consolidado/df_final_historico.parquet`
               - **Fonte principal** de dados histÃ³ricos
               - ContÃ©m todos os anos consolidados
            
                3. **Terceira opÃ§Ã£o**: `dados/TC_Ext/{ANO}/df_final.parquet`
               - Dados especÃ­ficos do ano (se filtro de ano aplicado)
            
            **Processo**:
            - Sistema carrega dados histÃ³ricos completos
            - Aplica filtros selecionados (Oficina, VeÃ­culo, Type 05, Type 06, etc.)
            - Filtra pelos perÃ­odos selecionados para cÃ¡lculo de mÃ©dia
            - Remove perÃ­odos excluÃ­dos (meses_excluir_media)
            """)
        
        with st.expander("**3ï¸âƒ£ CÃ¡lculo de MÃ©dias HistÃ³ricas**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o `calcular_medias_forecast()` nas pÃ¡ginas 2 e 3
            
            **Processo**:
            1. **Filtrar dados pelos perÃ­odos selecionados**:
               - Apenas perÃ­odos marcados para mÃ©dia sÃ£o considerados
               - PerÃ­odos excluÃ­dos sÃ£o removidos
            
            2. **Agrupar por chaves Ãºnicas**:
               - Chaves: `['Oficina', 'VeÃ­culo', 'Tipo_Custo', 'Type 06', ...]`
               - Cada combinaÃ§Ã£o Ãºnica gera uma linha no forecast
            
            3. **Calcular mÃ©dia por grupo**:
               - Soma dos valores histÃ³ricos / nÃºmero de perÃ­odos
               - Usa coluna `Total` (nunca `Valor`)
            
            4. **Calcular volume mÃ©dio histÃ³rico**:
               - Mesma lÃ³gica: agrupa e calcula mÃ©dia de volumes
               - Usado para calcular proporÃ§Ãµes futuras
            
            **Resultado**: DataFrame com mÃ©dias histÃ³ricas por combinaÃ§Ã£o de chaves
            """)
        
        with st.expander("**4ï¸âƒ£ CÃ¡lculo do Forecast**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o `calcular_forecast_completo()` nas pÃ¡ginas 2 e 3
            
            **Processo (linha a linha)**:
            1. **Para cada linha do forecast**:
               - ObtÃ©m mÃ©dia histÃ³rica da combinaÃ§Ã£o de chaves
               - ObtÃ©m volume do mÃªs futuro
               - ObtÃ©m volume mÃ©dio histÃ³rico
            
            2. **Calcula proporÃ§Ã£o de volume**:
               ```
               proporÃ§Ã£o = Volume_MÃªs_Futuro / Volume_MÃ©dio_HistÃ³rico
               ```
            
            3. **Calcula variaÃ§Ã£o percentual**:
               ```
               variaÃ§Ã£o = proporÃ§Ã£o - 1.0
               ```
            
            4. **Aplica sensibilidade**:
               - Se `Tipo_Custo == 'Fixo'`: usa `sensibilidade_fixo`
               - Se `Tipo_Custo == 'VariÃ¡vel'`: usa `sensibilidade_variavel`
               - Se modo Type 06: usa sensibilidade especÃ­fica do Type 06
               ```
               variaÃ§Ã£o_ajustada = variaÃ§Ã£o * sensibilidade
               ```
            
            5. **Calcula forecast**:
               ```
               fator_variaÃ§Ã£o = 1.0 + variaÃ§Ã£o_ajustada
               fator_inflaÃ§Ã£o = 1.0 + (inflaÃ§Ã£o / 100.0)
               forecast = MÃ©dia_HistÃ³rica * fator_variaÃ§Ã£o * fator_inflaÃ§Ã£o
               ```
            
            **Resultado**: DataFrame completo com forecast linha a linha
            """)
        
        with st.expander("**5ï¸âƒ£ Salvamento dos Arquivos**", expanded=False):
            st.markdown("""
            **Onde**: FunÃ§Ã£o de salvamento nas pÃ¡ginas 2 e 3
            
            **Processo**:
            1. **Verificar/Criar pasta Forecast**:
                    - Verifica se `dados/TC_Ext/Forecast/` existe
               - Se nÃ£o existe, cria automaticamente: `os.makedirs(pasta_forecast, exist_ok=True)`
            
            2. **Salvar forecast_completo.parquet**:
               - Arquivo principal com todas as linhas do forecast
               - Substitui arquivo anterior (nÃ£o concatena)
               - LocalizaÃ§Ã£o: `dados/TC_Ext/Forecast/forecast_completo.parquet`
            
            3. **Salvar forecast_historico.parquet** (se aplicÃ¡vel):
               - HistÃ³rico de forecasts gerados anteriormente
               - Pode ser concatenado com novo forecast
            
            4. **Salvar forecast_previsao.parquet** (se aplicÃ¡vel):
               - Apenas previsÃµes futuras (sem dados histÃ³ricos)
            
            **IMPORTANTE**: 
            - Arquivos sÃ£o **substituÃ­dos** a cada geraÃ§Ã£o (nÃ£o concatenados como histÃ³rico)
            - Cada geraÃ§Ã£o cria um forecast novo baseado nas configuraÃ§Ãµes atuais
            - Arquivos antigos sÃ£o sobrescritos
            """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 3: PÃ¡gina 2 - Best Estimate Simulador
        st.markdown("## ðŸ”® PÃGINA 2 - BEST ESTIMATE SIMULADOR {#pagina-simulador}")
        
        st.markdown("""
        ### Funcionalidades Principais
        
        **Objetivo:**
        A pÃ¡gina 2 (Best Estimate - Simulador) permite **simular e ajustar** parÃ¢metros do forecast
        em tempo real, visualizando o impacto das mudanÃ§as antes de salvar.
        
        **Funcionalidades:**
        
        **1. ConfiguraÃ§Ã£o Interativa de ParÃ¢metros:**
        - SeleÃ§Ã£o de perÃ­odos histÃ³ricos para mÃ©dia (multiselect)
        - ExclusÃ£o de meses especÃ­ficos (multiselect)
        - ConfiguraÃ§Ã£o de sensibilidades (fixo, variÃ¡vel, Type 06)
        - ConfiguraÃ§Ã£o de inflaÃ§Ã£o (global e por Type 06)
        - SeleÃ§Ã£o de perÃ­odos futuros para forecast
        
        **2. VisualizaÃ§Ã£o em Tempo Real:**
        - GrÃ¡ficos atualizados automaticamente ao alterar parÃ¢metros
        - Tabelas interativas mostrando valores linha a linha
        - ComparaÃ§Ã£o entre diferentes cenÃ¡rios
        
        **3. Ajustes de Volume:**
        - Permite ajustar volumes futuros manualmente
        - Visualiza impacto imediato nos custos previstos
        - Suporta diferentes volumes por perÃ­odo
        
        **4. Salvamento de Forecast:**
        - BotÃ£o para salvar forecast calculado
        - Salva em `dados/TC_Ext/Forecast/forecast_completo.parquet`
        - Substitui forecast anterior
        
        **5. AnÃ¡lise de Sensibilidade:**
        - Permite testar diferentes valores de sensibilidade
        - Visualiza impacto de mudanÃ§as nos parÃ¢metros
        - Ãštil para cenÃ¡rios "what-if"
        
        **6. Custos EspecÃ­ficos (BE Manual):**
        - Permite adicionar custos especÃ­ficos com valores manuais
        - Suporta dois tipos de aplicaÃ§Ã£o:
          - **Pontual**: Aplicado em meses especÃ­ficos selecionados
          - **Constante**: Aplicado a partir de um mÃªs inicial em diante
        - Rateio automÃ¡tico por veÃ­culo baseado em percentuais do arquivo de rateio
        - IntegraÃ§Ã£o automÃ¡tica com Account (Type 07) para buscar Type 06, Type 05, Custo e USI
        - VisualizaÃ§Ã£o e exclusÃ£o de custos especÃ­ficos cadastrados
        - FormataÃ§Ã£o numÃ©rica com separador de milhares (formato brasileiro)
        - Tabela interativa com seleÃ§Ã£o mÃºltipla para exclusÃ£o em lote
        - Os custos especÃ­ficos sÃ£o marcados como "BE Manual" na coluna Tipo
        - Integrados automaticamente ao forecast final como linhas separadas
        
        **7. Nomenclatura Atualizada:**
        - Coluna "Tipo" agora usa "BE" para forecast normal
        - Coluna "Tipo" usa "BE Manual" para custos especÃ­ficos/manuais
        - TÃ­tulo atualizado: "Best Estimate - PrevisÃ£o de Custo Total"
        - Compatibilidade automÃ¡tica com arquivos antigos (conversÃ£o de "Forecast" para "BE")
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 3.1: Custos EspecÃ­ficos - Detalhamento
        st.markdown("### ðŸ’° Custos EspecÃ­ficos (BE Manual) - Detalhamento")
        
        st.markdown("""
        **Funcionalidade:** Permite adicionar custos especÃ­ficos com valores manuais que sÃ£o integrados ao forecast.
        
        **Como Funciona:**
        
        **1. Adicionar Custo EspecÃ­fico:**
        - Acesse a aba "âž• Adicionar Custo" na pÃ¡gina 2
        - Preencha os campos obrigatÃ³rios:
          - **Account (Type 07)**: Seleciona o Account e busca automaticamente Type 06, Type 05, Custo e USI
          - **Oficina**: Seleciona a oficina (sem opÃ§Ã£o "Todos")
          - **VeÃ­culo**: Seleciona veÃ­culo especÃ­fico ou "Todos" para rateio automÃ¡tico
          - **PerÃ­odo**: Seleciona o perÃ­odo (mÃªs e ano)
          - **Tipo de AplicaÃ§Ã£o**: 
            - **Pontual**: Aplicado apenas nos meses selecionados
            - **Constante**: Aplicado a partir do mÃªs inicial em diante
          - **Valor Total**: Valor total do custo
          - **DescriÃ§Ã£o**: DescriÃ§Ã£o opcional do custo
        
        **2. Rateio AutomÃ¡tico:**
        - Se "Todos" for selecionado para VeÃ­culo, o sistema busca automaticamente os percentuais de rateio do arquivo `Reporting fluxo anexo.xlsx` (aba "Rateio")
        - O rateio Ã© aplicado mÃªs a mÃªs conforme os percentuais do arquivo
        - Se um veÃ­culo especÃ­fico for selecionado, o rateio Ã© 100% para aquele veÃ­culo
        - O valor total Ã© distribuÃ­do proporcionalmente entre os veÃ­culos
        
        **3. Visualizar Custos:**
        - Acesse a aba "ðŸ“‹ Visualizar Custos"
        - Tabela interativa com todas as colunas do formato `df_final_historico_forecast.xlsx`
        - FormataÃ§Ã£o numÃ©rica com 2 casas decimais e separador de milhares (formato brasileiro)
        - SeleÃ§Ã£o mÃºltipla com checkboxes para exclusÃ£o em lote
        - BotÃ£o "ðŸ—‘ï¸ Deletar Selecionadas" para remover custos
        
        **4. IntegraÃ§Ã£o com Forecast:**
        - Os custos especÃ­ficos sÃ£o automaticamente incluÃ­dos no forecast final
        - Aparecem como linhas separadas com Tipo = "BE Manual"
        - NÃ£o sÃ£o somados ao forecast calculado, mas adicionados como linhas independentes
        - MantÃ©m o mesmo formato e estrutura do forecast normal
        
        **5. PersistÃªncia:**
        - Os custos especÃ­ficos sÃ£o salvos em `dados/TC_Ext/Forecast/custos_especificos.parquet`
        - SÃ£o carregados automaticamente ao gerar o forecast
        - Permanecem salvos atÃ© serem explicitamente excluÃ­dos
        
        **6. Formato de Dados:**
        - Os custos especÃ­ficos seguem exatamente o formato de `df_final_historico_forecast.xlsx`
        - Colunas na ordem: Account, Ano, Centrocst, Custo, Fornec., Fornecedor, Mes, Oficina, PerÃ­odo, Soma_Percentuais, Tipo, Total, Type 05, Type 06, USI, Valor, VeÃ­culo
        - Tipo sempre preenchido como "BE Manual" para identificaÃ§Ã£o
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 4: PÃ¡gina - Best Estimate (AnÃ¡lise)
        st.markdown("## ðŸ“Š PÃGINA - BEST ESTIMATE (ANÃLISE) {#pagina-analise}")
        
        st.markdown("""
        ### Funcionalidades Principais
        
        **Objetivo:**
        A pÃ¡gina **Best Estimate (AnÃ¡lise)** no menu **TC Ext** substitui a anÃ¡lise legacy e entrega:
        - as **mesmas tabelas/visuais** da Home (TC Ext),
        - porÃ©m alimentadas pelos **arquivos de Forecast** gerados pelo simulador.
        
        **Funcionalidades:**
        
        **1. Fonte de dados (Forecast):**
        - LÃª `dados/TC_Ext/Forecast/forecast_completo.parquet` (custos) e `dados/TC_Ext/Forecast/df_vol_historico.parquet` (volume)
        - Permite analisar previsÃµes (BE) e histÃ³rico no mesmo layout
        - Expander de diagnÃ³stico mostra paths, mtimes e contagens
        
        **2. VisualizaÃ§Ãµes (mesma base da Home):**
        - GrÃ¡ficos e tabelas por perÃ­odo, oficina, veÃ­culo
        - Mesmo padrÃ£o de filtros e formataÃ§Ã£o
        - Sem â€œcorteâ€ de meses futuros quando houver Forecast
        
        **3. Tabelas detalhadas (com TOTAL coerente):**
        - No modo CPU, totais sÃ£o sempre `CPU = sum(Total) / sum(Volume)` (ponderado)
        - Expander opcional â€œVolume por perÃ­odoâ€ ajuda a explicar variaÃ§Ãµes do TOTAL mÃªs a mÃªs
        
        **4. ComparaÃ§Ãµes:**
        - Permite comparar BE vs histÃ³rico dentro do mesmo layout de anÃ¡lise
        - Facilita validar premissas (sensibilidade/inflaÃ§Ã£o) pela variaÃ§Ã£o temporal
        
        **5. IntegraÃ§Ã£o com o simulador:**
        - O simulador gera/salva os arquivos em `dados/TC_Ext/Forecast/`
        - A anÃ¡lise lÃª esses arquivos e atualiza as visualizaÃ§Ãµes
        
        **6. Modos de visualizaÃ§Ã£o:**
        - **Custo Total:** Valores absolutos em R$
        - **CPU (Custo por Unidade):** Valores por unidade produzida
        - Permite alternar entre modos para diferentes anÃ¡lises
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 5: Fluxo de Dados e Processamento
        st.markdown("## ðŸ”„ FLUXO DE DADOS E PROCESSAMENTO {#fluxo-dados-forecast}")
        
        st.markdown("""
        ### Fluxo Completo de Dados
        
        **Diagrama de Fluxo:**
        ```
        Dados HistÃ³ricos (dados/TC_Ext/historico_consolidado/)
                â”‚
                â”œâ”€â”€> Carregamento (load_data)
                â”‚       â”‚
                â”‚       â”œâ”€â”€> Aplicar Filtros (Oficina, VeÃ­culo, etc.)
                â”‚       â”‚
                â”‚       â””â”€â”€> Filtrar PerÃ­odos Selecionados
                â”‚
                â”œâ”€â”€> CÃ¡lculo de MÃ©dias (calcular_medias_forecast)
                â”‚       â”‚
                â”‚       â”œâ”€â”€> Agrupar por Chaves Ãšnicas
                â”‚       â”‚
                â”‚       â”œâ”€â”€> Calcular MÃ©dia de Custos
                â”‚       â”‚
                â”‚       â””â”€â”€> Calcular Volume MÃ©dio HistÃ³rico
                â”‚
                â”œâ”€â”€> CÃ¡lculo de Forecast (calcular_forecast_completo)
                â”‚       â”‚
                â”‚       â”œâ”€â”€> Para cada linha:
                â”‚       â”‚       â”œâ”€â”€> Obter MÃ©dia HistÃ³rica
                â”‚       â”‚       â”œâ”€â”€> Obter Volume Futuro
                â”‚       â”‚       â”œâ”€â”€> Calcular ProporÃ§Ã£o
                â”‚       â”‚       â”œâ”€â”€> Aplicar Sensibilidade
                â”‚       â”‚       â””â”€â”€> Aplicar InflaÃ§Ã£o
                â”‚       â”‚
                â”‚       â””â”€â”€> DataFrame Completo com Forecast
                â”‚
                â””â”€â”€> Salvamento (dados/TC_Ext/Forecast/)
                        â”‚
                        â”œâ”€â”€> forecast_completo.parquet
                        â”œâ”€â”€> forecast_historico.parquet
                        â””â”€â”€> forecast_previsao.parquet
        ```
        
        **CaracterÃ­sticas do Fluxo:**
        - **Tempo Real:** Forecast Ã© calculado em tempo real com configuraÃ§Ãµes atuais
        - **NÃ£o Persistente:** ConfiguraÃ§Ãµes (sensibilidade, inflaÃ§Ã£o) nÃ£o sÃ£o salvas, apenas o resultado
        - **SubstituiÃ§Ã£o:** Cada geraÃ§Ã£o substitui o forecast anterior
        - **IndependÃªncia:** Cada pÃ¡gina pode gerar seu prÃ³prio forecast
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 6: Arquivos Gerados
        st.markdown("## ðŸ“„ ARQUIVOS GERADOS {#arquivos-gerados-forecast}")
        
        st.markdown("""
        ### Arquivos na Pasta `dados/TC_Ext/Forecast/`
        
        **1. forecast_completo.parquet**
        - **ConteÃºdo**: Forecast completo com todas as linhas calculadas
        - **Estrutura**: Mesmas colunas dos dados histÃ³ricos + colunas de forecast
        - **Uso**: Fonte principal para anÃ¡lises e visualizaÃ§Ãµes
        - **AtualizaÃ§Ã£o**: SubstituÃ­do a cada geraÃ§Ã£o de forecast
        
        **2. forecast_historico.parquet**
        - **ConteÃºdo**: HistÃ³rico de forecasts gerados anteriormente
        - **Estrutura**: Similar ao forecast_completo, mas com mÃºltiplos forecasts
        - **Uso**: AnÃ¡lise de evoluÃ§Ã£o de forecasts ao longo do tempo
        - **AtualizaÃ§Ã£o**: Pode ser concatenado ou substituÃ­do (depende da implementaÃ§Ã£o)
        
        **3. forecast_previsao.parquet**
        - **ConteÃºdo**: Apenas previsÃµes futuras (sem dados histÃ³ricos)
        - **Estrutura**: Apenas perÃ­odos futuros do forecast
        - **Uso**: AnÃ¡lise focada apenas em previsÃµes
        - **AtualizaÃ§Ã£o**: SubstituÃ­do a cada geraÃ§Ã£o
        
        **4. df_final_historico_forecast.parquet**
        - **ConteÃºdo**: Dados histÃ³ricos filtrados usados para calcular o forecast
        - **Estrutura**: Dados histÃ³ricos apÃ³s aplicaÃ§Ã£o de filtros e seleÃ§Ã£o de perÃ­odos
        - **Uso**: ReferÃªncia dos dados que foram usados para calcular a mÃ©dia
        - **AtualizaÃ§Ã£o**: Gerado junto com o forecast
        
        **5. df_vol_historico.parquet**
        - **ConteÃºdo**: Volumes histÃ³ricos usados para cÃ¡lculo de proporÃ§Ãµes
        - **Estrutura**: Volumes por perÃ­odo, oficina, veÃ­culo
        - **Uso**: CÃ¡lculo de volume mÃ©dio histÃ³rico e proporÃ§Ãµes
        - **AtualizaÃ§Ã£o**: Pode ser copiado do histÃ³rico consolidado ou gerado
        
        **6. custos_especificos.parquet**
        - **ConteÃºdo**: Custos especÃ­ficos cadastrados manualmente (BE Manual)
        - **Estrutura**: Mesmo formato de `df_final_historico_forecast.xlsx` com coluna Tipo = "BE Manual"
        - **Uso**: Armazena custos especÃ­ficos que sÃ£o integrados ao forecast final
        - **AtualizaÃ§Ã£o**: Criado/modificado ao adicionar ou excluir custos especÃ­ficos
        - **LocalizaÃ§Ã£o**: `dados/TC_Ext/Forecast/custos_especificos.parquet`
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 6.1: Nomenclatura e Tipos
        st.markdown("### ðŸ·ï¸ Nomenclatura e Tipos de Dados")
        
        st.markdown("""
        **Coluna "Tipo" no Forecast:**
        
        O sistema utiliza a coluna "Tipo" para identificar diferentes tipos de dados no forecast:
        
        - **"HistÃ³rico"**: Dados histÃ³ricos reais (nÃ£o previstos)
        - **"BE"**: Best Estimate - Forecast calculado automaticamente pelo sistema
        - **"BE Manual"**: Best Estimate Manual - Custos especÃ­ficos adicionados manualmente
        
        **Compatibilidade:**
        - Arquivos antigos com "Forecast" sÃ£o automaticamente convertidos para "BE" ao carregar
        - Isso garante compatibilidade com versÃµes anteriores do sistema
        
        **Filtros e SeparaÃ§Ã£o:**
        - O sistema separa automaticamente histÃ³rico, BE e BE Manual ao gerar arquivos
        - `forecast_historico.parquet`: Apenas dados histÃ³ricos
        - `forecast_previsao.parquet`: Apenas BE e BE Manual (previsÃµes)
        - `df_final_historico_forecast.parquet`: Consolidado com todos os tipos
        """)
        
        st.markdown("---")
        
        # SeÃ§Ã£o 7: CenÃ¡rios de Uso
        st.markdown("## ðŸ“‹ CENÃRIOS DE USO {#cenarios-uso-forecast}")
        
        st.markdown("### Casos PrÃ¡ticos Completos")
        
        with st.expander("**CenÃ¡rio 1: Gerar Forecast pela Primeira Vez**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: Nunca gerou forecast, precisa criar previsÃµes para prÃ³ximos meses
            
            **Passo a Passo**:
            
            1. **Acessar PÃ¡gina 2 (Simulador)**:
               - Selecionar perÃ­odos histÃ³ricos (ex: Ãºltimos 3 meses)
               - Configurar sensibilidades (Fixo: 0%, VariÃ¡vel: 100%)
               - Configurar inflaÃ§Ã£o (ex: 5%)
               - Selecionar perÃ­odos futuros (ex: prÃ³ximos 6 meses)
            
            2. **Informar Volumes Futuros**:
               - Inserir volumes esperados para cada perÃ­odo futuro
               - Ou usar volumes projetados automaticamente
            
            3. **Visualizar Resultados**:
               - Verificar grÃ¡ficos e tabelas
               - Ajustar parÃ¢metros se necessÃ¡rio
            
            4. **Salvar Forecast**:
               - Clicar em "Salvar Forecast"
                    - Sistema cria `dados/TC_Ext/Forecast/` automaticamente
               - Salva `forecast_completo.parquet`
            
            5. **Analisar na PÃ¡gina 3**:
               - Acessar PÃ¡gina 3 (AnÃ¡lise)
               - Carregar forecast gerado
               - Visualizar anÃ¡lises detalhadas
            
            **Resultado**: 
            - Pasta `dados/TC_Ext/Forecast/` criada com forecast completo
            - Forecast disponÃ­vel para anÃ¡lises e comparaÃ§Ãµes
            """)
        
        with st.expander("**CenÃ¡rio 2: Atualizar Forecast com Novos Dados**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: JÃ¡ existe forecast, mas novos dados histÃ³ricos foram adicionados
            
            **Passo a Passo**:
            
            1. **Atualizar Dados HistÃ³ricos** (se necessÃ¡rio):
               - Executar extraÃ§Ã£o de dados (PÃ¡gina 5) para incluir novos perÃ­odos
               - HistÃ³rico consolidado Ã© atualizado automaticamente
            
            2. **Acessar PÃ¡gina 2 ou 3**:
               - Selecionar novos perÃ­odos histÃ³ricos (incluindo os mais recentes)
               - Manter ou ajustar sensibilidades e inflaÃ§Ã£o
            
            3. **Gerar Novo Forecast**:
               - Clicar em "Gerar Forecast" ou "Salvar Forecast"
               - Sistema recalcula com dados atualizados
            
            4. **Forecast Anterior Ã© SubstituÃ­do**:
               - `forecast_completo.parquet` Ã© sobrescrito
               - Novo forecast reflete dados mais recentes
            
            **Resultado**: 
            - Forecast atualizado com dados mais recentes
            - PrevisÃµes mais acuradas baseadas em histÃ³rico expandido
            """)
        
        with st.expander("**CenÃ¡rio 3: Testar Diferentes CenÃ¡rios (What-If)**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: Quer testar impacto de diferentes volumes ou inflaÃ§Ãµes
            
            **Passo a Passo**:
            
            1. **Acessar PÃ¡gina 2 (Simulador)**:
               - Configurar parÃ¢metros base (sensibilidades, perÃ­odos histÃ³ricos)
            
            2. **Testar CenÃ¡rio 1**:
               - Ajustar volumes futuros (ex: +10%)
               - Visualizar impacto nos custos
               - **NÃƒO salvar** (apenas visualizar)
            
            3. **Testar CenÃ¡rio 2**:
               - Ajustar volumes futuros (ex: -5%)
               - Visualizar impacto
               - Comparar com CenÃ¡rio 1
            
            4. **Testar Diferentes InflaÃ§Ãµes**:
               - Alterar percentual de inflaÃ§Ã£o
               - Ver impacto em todos os custos
               - Comparar cenÃ¡rios
            
            5. **Salvar CenÃ¡rio Escolhido**:
               - ApÃ³s decidir qual cenÃ¡rio usar
               - Configurar parÃ¢metros finais
               - Salvar forecast
            
            **Resultado**: 
            - MÃºltiplos cenÃ¡rios testados sem salvar
            - Forecast final salvo com cenÃ¡rio escolhido
            """)
        
        with st.expander("**CenÃ¡rio 4: AnÃ¡lise Detalhada de Forecast Gerado**", expanded=False):
            st.markdown("""
            **SituaÃ§Ã£o**: Forecast jÃ¡ foi gerado, precisa de anÃ¡lises detalhadas
            
            **Passo a Passo**:
            
            1. **Acessar PÃ¡gina 3 (AnÃ¡lise)**:
               - Sistema carrega `forecast_completo.parquet` automaticamente
               - Mostra data de Ãºltima atualizaÃ§Ã£o
            
            2. **Aplicar Filtros**:
               - Filtrar por Oficina, VeÃ­culo, Type 05, Type 06, etc.
               - Selecionar perÃ­odos especÃ­ficos
            
            3. **Visualizar GrÃ¡ficos**:
               - GrÃ¡ficos de linha mostrando evoluÃ§Ã£o
               - GrÃ¡ficos de barras comparando perÃ­odos
               - Identificar tendÃªncias e padrÃµes
            
            4. **Analisar Tabelas**:
               - Tabelas hierÃ¡rquicas com drill-down
               - Detalhamento linha a linha
               - Identificar maiores custos previstos
            
            5. **Exportar para Excel**:
               - Exportar tabelas para anÃ¡lise externa
               - Compartilhar resultados com equipe
            
            **Resultado**: 
            - AnÃ¡lises detalhadas do forecast
            - Insights para tomada de decisÃ£o
            - DocumentaÃ§Ã£o dos resultados
            """)
        
        st.markdown("---")
        
        st.success("""
        **âœ… Este capÃ­tulo descreve completamente a estrutura, atualizaÃ§Ã£o e funcionamento**
        das pÃ¡ginas de Best Estimate. Use estas informaÃ§Ãµes para entender como o sistema
        organiza os dados, processa os forecasts e como cada pÃ¡gina contribui para o processo.
        """)

# ==========================================
# SEÃ‡ÃƒO 6: APRESENTAÃ‡ÃƒO VISUAL
# ==========================================
elif indice_selecionado == "ðŸ“Š ApresentaÃ§Ã£o Visual":
    st.header("ðŸ“Š ApresentaÃ§Ã£o Visual - 5 Minutos")
    render_presentation_section(str(versao_atual), data_atualizacao)

# ==========================================
# SEÃ‡ÃƒO 7: CHATBOT DE DOCUMENTAÃ‡ÃƒO
# ==========================================
elif indice_selecionado == "ðŸ’¬ Chatbot de DocumentaÃ§Ã£o":
    st.header("ðŸ’¬ Chatbot de DocumentaÃ§Ã£o")
    
    st.markdown("""
    <div style="padding: 1.5rem; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); border-radius: 10px; margin-bottom: 2rem; color: white;">
        <h2 style="color: white; margin: 0;">ðŸ’¬ Assistente Virtual de DocumentaÃ§Ã£o</h2>
        <p style="color: #f0f0f0; margin: 0.5rem 0 0 0;">
            FaÃ§a perguntas sobre o sistema e receba respostas baseadas na documentaÃ§Ã£o completa
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Importar chatbot
    try:
        # Adicionar diretÃ³rio raiz ao path para importar chatbot
        import sys
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if base_path not in sys.path:
            sys.path.insert(0, base_path)
        from chatbot_documentacao import responder_pergunta
        
        # Inicializar histÃ³rico de conversa
        if 'historico_chat' not in st.session_state:
            st.session_state.historico_chat = []
        
        # Exibir histÃ³rico
        st.subheader("ðŸ’¬ Conversa")
        
        if st.session_state.historico_chat:
            for i, (pergunta, resposta, score) in enumerate(st.session_state.historico_chat):
                with st.expander(f"â“ {pergunta[:50]}...", expanded=False):
                    st.markdown(f"**Pergunta:** {pergunta}")
                    st.markdown(f"**Resposta:**")
                    st.markdown(resposta)
                    if score > 0:
                        st.caption(f"RelevÃ¢ncia: {score:.0%}")
        else:
            st.info("ðŸ’¡ FaÃ§a sua primeira pergunta abaixo para comeÃ§ar!")
        
        st.markdown("---")
        
        # Campo de entrada
        st.subheader("ðŸ“ FaÃ§a uma Pergunta")
        
        pergunta = st.text_input(
            "Digite sua pergunta sobre o sistema:",
            placeholder="Ex: Como funciona o Best Estimate? O que Ã© Flex Bud? Como processar dados?",
            key="input_pergunta"
        )
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            botao_perguntar = st.button("ðŸ” Buscar Resposta", type="primary", use_container_width=True)
        
        with col2:
            botao_limpar = st.button("ðŸ—‘ï¸ Limpar HistÃ³rico", use_container_width=True)
        
        if botao_limpar:
            st.session_state.historico_chat = []
            st.rerun()
        
        if botao_perguntar and pergunta:
            with st.spinner("ðŸ” Buscando na documentaÃ§Ã£o..."):
                resultado = responder_pergunta(pergunta)
                
                if resultado['resposta']:
                    # Adicionar ao histÃ³rico
                    st.session_state.historico_chat.append((
                        pergunta,
                        resultado['resposta'],
                        resultado['score']
                    ))
                    
                    # Exibir resposta
                    st.success("âœ… Resposta encontrada!")
                    st.markdown("**Resposta:**")
                    st.markdown(resultado['resposta'])
                    
                    if resultado['score'] > 0:
                        st.caption(f"ðŸ“Š RelevÃ¢ncia da resposta: {resultado['score']:.0%}")
                    
                    # Exibir segmentos adicionais se houver
                    if resultado['segmentos_encontrados']:
                        st.markdown("---")
                        st.subheader("ðŸ“š InformaÃ§Ãµes Adicionais")
                        for i, segmento in enumerate(resultado['segmentos_encontrados'], 1):
                            with st.expander(f"InformaÃ§Ã£o adicional {i}", expanded=False):
                                st.markdown(segmento)
                    
                    st.rerun()
        
        # SugestÃµes de perguntas
        st.markdown("---")
        st.subheader("ðŸ’¡ Perguntas Sugeridas")
        
        perguntas_sugeridas = [
            "O que Ã© o Stellantis Cost Intelligence (SCI)?",
            "Como funciona o Best Estimate?",
            "O que Ã© Flex Bud?",
            "Como funciona o rateio por veÃ­culo?",
            "Qual a diferenÃ§a entre TC Ext e TC VeÃ­culos?",
            "Como funciona a sensibilidade no simulador?",
            "O que Ã© CPU (Custo por Unidade)?",
            "Como funciona o Waterfall?",
        ]
        
        cols = st.columns(2)
        for i, pergunta_sugerida in enumerate(perguntas_sugeridas):
            with cols[i % 2]:
                if st.button(f"â“ {pergunta_sugerida}", key=f"sug_{i}", use_container_width=True):
                    # Processar pergunta sugerida diretamente
                    with st.spinner("ðŸ” Buscando na documentaÃ§Ã£o..."):
                        resultado = responder_pergunta(pergunta_sugerida)
                        
                        if resultado['resposta']:
                            # Adicionar ao histÃ³rico
                            st.session_state.historico_chat.append((
                                pergunta_sugerida,
                                resultado['resposta'],
                                resultado['score']
                            ))
                            st.rerun()
        
    except ImportError as e:
        st.error(f"âŒ Erro ao importar mÃ³dulo de chatbot: {str(e)}")
        st.info("ðŸ’¡ Certifique-se de que o arquivo chatbot_documentacao.py existe na raiz do projeto.")
    except Exception as e:
        st.error(f"âŒ Erro no chatbot: {str(e)}")
        import traceback
        st.code(traceback.format_exc())

# ==========================================
# SEÃ‡ÃƒO 8: SISTEMA DE ALERTAS
# ==========================================
elif indice_selecionado == "ðŸ”” Sistema de Alertas":
    st.header("ðŸ”” Sistema de Alertas")

    st.markdown(
        """
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #ff6b6b 0%, #feca57 100%); border-radius: 10px; margin-bottom: 1.25rem; color: white;">
            <h2 style="color: white; margin: 0;">ðŸ”” Central de Alertas (TC VeÃ­culos)</h2>
            <p style="color: #fff; opacity: 0.92; margin: 0.5rem 0 0 0;">
                Monitoramento automÃ¡tico de desvios relevantes no TC VeÃ­culos â€” com ranking consolidado e notificaÃ§Ãµes.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
### âœ… Objetivo
Detectar rapidamente **anomalias / perdas** no custo do TC VeÃ­culos, priorizando o que mais impacta o resultado.

O sistema gera um **ranking hierÃ¡rquico**:
**Type 05 â†’ Type 06 â†’ Account â†’ Oficinas** (com texto breve quando disponÃ­vel).

### ðŸ“ Onde fica no app
No menu lateral do SCI existem duas pÃ¡ginas:
- **Central de Alertas â†’ Monitoramento** (`alertas/alert_ui.py`)
- **Central de Alertas â†’ ConfiguraÃ§Ã£o de Alertas** (`alertas/alert_config_ui.py`)

### ðŸ”Ž O que o alerta compara
O motor suporta dois modos:
1. **Budget Flex Ã— Real** *(principal)*
2. **MÃªs Ã— MÃªs Anterior** *(secundÃ¡rio)*

> ObservaÃ§Ã£o: no modo **Budget Flex Ã— Real**, o â€œesperadoâ€ vem do cÃ¡lculo de Flex BUD detalhado (reuso da base do TC VeÃ­culos).
        """
    )

    with st.expander("ðŸ§  Como o motor funciona (visÃ£o geral)", expanded=False):
        st.markdown(
            """
**Fonte de dados (TC VeÃ­culos):** parquets consolidados em `dados/TC_Principal/historico_consolidado/`.

**Etapas (alto nÃ­vel):**
1. Carrega Real, Volume Real, Budget e Volume Budget
2. Calcula Flex BUD detalhado com dimensÃµes (Oficina / Type 05 / Type 06 / Account)
3. Aplica filtros da regra (Oficina, Type 05, Type 06, Account)
4. Calcula **Real vs Esperado** e ranqueia os **Top N Type 06** com maior perda
5. Consolida em um **card Ãºnico** (drill-down hierÃ¡rquico)

**Severidade (padrÃ£o):** classificada por desvio percentual absoluto:
- **CrÃ­tico:** â‰¥ 15%
- **Moderado:** â‰¥ 5%
- **Informativo:** < 5%

**Base tÃ©cnica principal:** `alertas/alert_engine.py`

**FunÃ§Ãµes mais importantes:**
- `calcular_ranking_consolidado()` â€” monta o card hierÃ¡rquico Ãºnico usado no monitoramento e nas notificaÃ§Ãµes.
- `gerar_tabela_validacao()` â€” gera a conferÃªncia `Type 05 / Type 06 / Account / Flex BUD / Real / Delta / % Delta`.
- `evaluate_rule()` e `evaluate_all_rules()` â€” avaliam regras ativas e retornam a estrutura final do alerta.
            """
        )

    with st.expander("âš™ï¸ ConfiguraÃ§Ã£o de regras", expanded=False):
        st.markdown(
            """
Em **ConfiguraÃ§Ã£o de Alertas**, Ã© possÃ­vel criar regras com:
- **Ano** e **modo de comparaÃ§Ã£o**
- **Top N** (quantos Type 06 destacar)
- **Moeda** (BRL / EUR / USD)
- Filtros opcionais em cascata: **Type 05**, **Type 06**, **Account**
- Filtro opcional de **Oficinas** (vazio = todas)

Cada regra pode ser **ativada/desativada** e removida.

**ObservaÃ§Ã£o importante:** hoje o SCI trabalha principalmente com **disparo manual ou pÃ³s-processamento**. A regra continua armazenando metadados de agenda por compatibilidade, mas o fluxo operacional atual privilegia o acionamento quando a base jÃ¡ foi processada e estÃ¡ pronta para leitura.
            """
        )

    with st.expander("ðŸ“Š Tabela de validaÃ§Ã£o", expanded=False):
        st.markdown(
            """
Essa tabela existe para permitir **auditoria rÃ¡pida do cÃ¡lculo** antes ou depois do envio do alerta.

**Colunas principais:**
- `Type 05`
- `Type 06`
- `Account`
- `Flex BUD`
- `Flex BUD P`
- `Real`
- `Real - Flex BUD P`
- `% Delta`

**Como interpretar:**
- `Flex BUD` = valor esperado integral
- `Flex BUD P` = valor esperado proporcional ao perÃ­odo corrente
- `Real - Flex BUD P` = desvio monetÃ¡rio principal
- `% Delta` = desvio percentual relativo ao esperado proporcional

**Melhoria recente do sistema:**
- o preenchimento de `Type 05` passou a ser preservado a partir do `flex_detalhado` e, quando necessÃ¡rio, complementado por mapeamento controlado com base em `Type 06 + Account`, evitando linhas vazias na validaÃ§Ã£o.

**Uso prÃ¡tico:**
- conferir se a perda identificada no card bate com a linha detalhada
- validar se a regra/filtro aplicado estÃ¡ trazendo o universo correto
- apoiar explicaÃ§Ã£o de desvio antes de acionar stakeholders
            """
        )

    with st.expander("ðŸ“¨ NotificaÃ§Ãµes (E-mail / Teams)", expanded=False):
        st.markdown(
            """
O sistema pode enviar o ranking consolidado para:
- **E-mail (Microsoft Graph / OAuth2)** â€” com autenticaÃ§Ã£o MSAL
- **Microsoft Teams (Webhook)** â€” com card hierÃ¡rquico formatado

TambÃ©m existe a opÃ§Ã£o de manter apenas o uso **interno no app** (sem envio).

Na aba **ðŸ§ª Testar Envio**, dÃ¡ para validar rapidamente se o Graph/Webhook estÃ£o corretos.

**Teams:**
- envia card consolidado com Ã¡rvore visual por `Type 05 -> Type 06 -> Account -> Oficina`
- inclui barra textual de representatividade do desvio total
- mantÃ©m legibilidade prÃ³xima da visualizaÃ§Ã£o interna do SCI

**E-mail:**
- usa HTML estruturado com ranking consolidado
- pode incluir a tabela de validaÃ§Ã£o junto do alerta
- autenticaÃ§Ã£o moderna via Microsoft Graph quando configurada
            """
        )

    with st.expander("â–¶ï¸ Fluxo operacional atual", expanded=False):
        st.markdown(
            """
O fluxo hoje foi simplificado para ficar mais aderente ao uso real do SCI:

- o usuÃ¡rio pode clicar em **Verificar agora** para gerar o ranking consolidado
- o usuÃ¡rio pode clicar em **Disparar alertas ativos** para forÃ§ar o envio manual
- o processamento de dados pode acionar a avaliaÃ§Ã£o dos alertas ao final da atualizaÃ§Ã£o da base

**Por que isso Ã© melhor:**
- reduz dependÃªncia de app aberto em um horÃ¡rio fixo
- garante que o alerta roda sobre a base mais recente
- deixa a operaÃ§Ã£o mais previsÃ­vel para fechamento e acompanhamento mensal
            """
        )

    with st.expander("ðŸ—‚ï¸ PersistÃªncia e auditoria", expanded=False):
        st.markdown(
            """
As configuraÃ§Ãµes e o histÃ³rico ficam salvos em JSON no pacote `alertas/`:
- `alertas/alert_rules.json` â€” regras + canais de notificaÃ§Ã£o + agenda
- `alertas/alert_log.json` â€” histÃ³rico de execuÃ§Ãµes (quando e o que foi enviado)

O histÃ³rico pode ser consultado na aba **ðŸ“œ HistÃ³rico** da pÃ¡gina de configuraÃ§Ã£o.
            """
        )

    with st.expander("ðŸ§© O que o usuÃ¡rio enxerga no monitoramento", expanded=False):
        st.markdown(
            """
Na pÃ¡gina de monitoramento, o SCI mostra um **card consolidado** em vez de vÃ¡rios alertas soltos.

**Estrutura do card:**
- agrupamento por `Type 05`
- detalhamento por `Type 06`
- drill-down atÃ© `Account` e `Oficina`
- `Texto breve` em lowercase para leitura mais limpa
- barra com percentual de participaÃ§Ã£o no **desvio total** do card

**Resultado para o usuÃ¡rio:**
- mais fÃ¡cil priorizar o que mais pesa no problema
- mais fÃ¡cil explicar o desvio para a operaÃ§Ã£o
- mais fÃ¡cil validar antes de enviar Teams/e-mail
            """
        )

# ==========================================
# ==========================================
# SEÃ‡ÃƒO: GUIA DE BUILD (EXE)
# ==========================================
elif indice_selecionado == "ðŸ“¦ Guia de Build (EXE)":
    st.header("ðŸ“¦ Guia de Build (EXE)")

    st.header("ðŸ“¦ Guia de Build â€” Empacotamento como ExecutÃ¡vel Windows")

    st.info(
        "Atualizado em 20/02/2026: este projeto gera EXE Windows usando `streamlit-desktop-app` "
        "(que internamente usa PyInstaller) e empacota o runtime em `_internal/`. "
        "Este passo a passo foi escrito para ser reusado em outro projeto e para uma LLM conseguir "
        "reconstruir o mesmo mÃ©todo com alta fidelidade."
    )

    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%); padding: 20px; border-radius: 10px; margin-bottom: 16px; color: white;">
        <h2 style="color: white; margin: 0;">ðŸ“¦ SCI â€” Guia de Empacotamento (EXE)</h2>
        <p style="color: #a0c4ff; margin: 0.5rem 0 0 0;">Passo a passo oficial (mesmo conteÃºdo do arquivo <code>GUIA_EXECUTAVEL.md</code>)</p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("0) VisÃ£o geral (mÃ©todo)", expanded=False):
        st.markdown("""
        **Objetivo:** gerar um executÃ¡vel Windows do Streamlit que abre como *desktop app*, sem depender do repositÃ³rio.

        **MÃ©todo adotado (o mesmo padrÃ£o do projeto referÃªncia):**
        - `streamlit-desktop-app build app.py --name <NOME>`
        - PÃ³s-build: copiar `dados/`, mÃ³dulos/pÃ¡ginas e scripts `.py` avulsos para `dist/<NOME>/_internal/`

        **ObservaÃ§Ã£o importante (AgGrid / st_aggrid):**
        - As pÃ¡ginas do Streamlit (multipage) sÃ£o carregadas em *runtime*.
        - Isso pode fazer o empacotador **nÃ£o incluir automaticamente** dependÃªncias importadas apenas nessas pÃ¡ginas.
        - SoluÃ§Ã£o robusta adotada no SCI: pÃ³s-build, copiar o pacote `st_aggrid` do `.venv` para dentro do `_internal/`.

        **Por que isso evita bugs no EXE:**
        - No executÃ¡vel, o caminho â€œrealâ€ do cÃ³digo empacotado Ã© `sys._MEIPASS` (pasta `_internal/`).
        - Qualquer lÃ³gica de `sys.path` baseada em `dirname(__file__)` precisa considerar `sys._MEIPASS`.
        """)

    with st.expander("1) PrÃ©-requisitos (ambiente)", expanded=False):
        st.markdown("""
        - Windows 10/11
        - Python (mesma versÃ£o usada no projeto, preferencialmente) + `venv`
        - DependÃªncias do projeto instaladas (`pip install -r requirements.txt`)
        """)

    with st.expander("2) Bibliotecas e ferramentas usadas", expanded=False):
        st.markdown("""
        **Ferramenta de build (principal):**
        - `streamlit-desktop-app`

        **Empacotador (indireto):**
        - PyInstaller (chamado pela ferramenta)

        **Desktop container:**
        - `pywebview` (pasta `webview/` aparece no `_internal/`)

        **Framework:**
        - `streamlit`

        > ObservaÃ§Ã£o: no nosso caso, o build falha se existir BOM (U+FEFF) no comeÃ§o do `app.py`.
        """)

    with st.expander("3) Passo crÃ­tico: remover BOM (U+FEFF) do app.py", expanded=False):
        st.markdown("""
        Se o build acusar:
        `SyntaxError: invalid non-printable character U+FEFF`

        Remova o BOM regravando em UTF-8 sem BOM.
        """)
        st.code(
            "$c = [System.IO.File]::ReadAllBytes('app.py'); "
            "if ($c.Length -ge 3 -and $c[0] -eq 0xEF -and $c[1] -eq 0xBB -and $c[2] -eq 0xBF) { "
            "  [System.IO.File]::WriteAllBytes('app.py', $c[3..($c.Length-1)]) ; "
            "  'OK: BOM removido' "
            "} else { 'OK: sem BOM' }",
            language="powershell",
        )

    with st.expander("4) ConstruÃ§Ã£o do build (comando oficial)", expanded=False):
        st.markdown("""
        Na raiz do projeto (mesma pasta do `app.py`), execute:
        """)
        st.code("streamlit-desktop-app build app.py --name Stellantis-Cost-Intelligence", language="powershell")
        st.markdown("""
        Depois disso, o diretÃ³rio `dist/Stellantis-Cost-Intelligence/` deve existir.

        > Nota: o `streamlit-desktop-app` **nÃ£o aceita** `--hidden-import` no CLI.
        > Para garantir dependÃªncias de pÃ¡ginas carregadas em runtime (ex.: `st_aggrid`), use o `build_exe.bat`.
        """)

    with st.expander("5) PÃ³s-build obrigatÃ³rio: copiar recursos para _internal/", expanded=False):
        st.markdown("""
        O runtime do EXE lÃª tudo de dentro de `dist/<NOME>/_internal/`.

        No SCI, nÃ³s copiamos para `_internal/`:
        - `dados/` (parquets, histÃ³ricos)
        - `pages/`, `tc_core/`, `tc_principal/`, `tc_ext/`, `.streamlit/`
        - scripts `.py` avulsos que sÃ£o importados em runtime (extraÃ§Ã£o, exports, versionamento)
        - JSONs e imagens necessÃ¡rias

        **AgGrid (streamlit-aggrid):**
        - Sintoma quando nÃ£o incluÃ­do: `mÃ³dulo 'st_aggrid' nÃ£o encontrado` e o sistema entra em fallback.
        - CorreÃ§Ã£o aplicada no SCI: copiar `st_aggrid/` e `streamlit_aggrid-*.dist-info/` do `.venv` para dentro do `_internal/`.

        Exemplo (PowerShell):
        """)
        st.code(
            "$dest = 'dist\\Stellantis-Cost-Intelligence\\_internal'\n"
            "Copy-Item '.venv\\Lib\\site-packages\\st_aggrid' -Destination ($dest + '\\st_aggrid') -Recurse -Force\n"
            "Copy-Item '.venv\\Lib\\site-packages\\streamlit_aggrid-*.dist-info' -Destination $dest -Recurse -Force\n",
            language="powershell",
        )
        st.markdown("""

        **Script oficial:** `build_exe.bat` (na raiz) automatiza isso.
        """)

    with st.expander("9) O que NÃƒO fazer (armadilha do .spec)", expanded=False):
        st.markdown("""
        Evite tentar rodar `pyinstaller` manualmente a partir do `.spec` gerado automaticamente pelo `streamlit-desktop-app`.

        **Por quÃª?** Esse `.spec` costuma referenciar um script temporÃ¡rio em `%TEMP%` (ex.: `tmp_xxx.py`).
        Depois do build, esse arquivo pode ser apagado, e o rebuild falha com:
        - `ERROR: script 'C:\\Users\\...\\AppData\\Local\\Temp\\tmp_XXXX.py' not found`

        **SoluÃ§Ã£o adotada:** nÃ£o rebuildar via `.spec`; em vez disso, fazer pÃ³s-build (cÃ³pias) para `_internal/`.
        """)

    with st.expander("6) Armadilha comum no EXE: sys.path e _MEIPASS", expanded=False):
        st.markdown("""
        **Sintoma:** no EXE, algumas telas funcionam, mas mÃ³dulos avulsos (ex.: `processamento_dados_veiculos.py`) â€œsomemâ€.

        **Causa:** pÃ¡ginas faziam `sys.path.insert(0, dirname(dirname(dirname(__file__))))`.
        No EXE isso aponta para a pasta do `.exe`, nÃ£o para `_internal/`.

        **CorreÃ§Ã£o padrÃ£o (reutilizÃ¡vel):**
        """)
        st.code(
            "import sys\n"
            "import os\n"
            "if hasattr(sys, '_MEIPASS'):\n"
            "    project_root = sys._MEIPASS\n"
            "else:\n"
            "    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\n"
            "if project_root not in sys.path:\n"
            "    sys.path.insert(0, project_root)\n",
            language="python",
        )

    with st.expander("7) ValidaÃ§Ã£o (checklist)", expanded=False):
        st.markdown("""
        - Abrir: `dist\\Stellantis-Cost-Intelligence\\Stellantis-Cost-Intelligence.exe`
        - Confirmar que o app abre (janela desktop) e/ou responde em `http://localhost:8501`
        - Testar uma extraÃ§Ã£o (Budget e Real) e confirmar geraÃ§Ã£o dos parquets por veÃ­culo:
          - `df_veiculos_custo_fp.parquet`
          - `df_veiculos_cpu.parquet`
        """
        )

    with st.expander("8) Guia completo (GUIA_EXECUTAVEL.md)", expanded=False):
        try:
            guia_path = os.path.join(get_base_path(), "GUIA_EXECUTAVEL.md")
            if os.path.exists(guia_path):
                guia_mtime = os.path.getmtime(guia_path)
                st.caption(
                    f"Fonte: {guia_path} | Atualizado em: {_formatar_mtime(guia_mtime)}"
                )
                st.markdown(_ler_arquivo_texto_cacheado(guia_path, guia_mtime))
            else:
                st.warning(
                    "GUIA_EXECUTAVEL.md nÃ£o foi encontrado. "
                    "No modo executÃ¡vel, ele deve estar dentro de _internal/. "
                    "Recrie o executÃ¡vel usando build_exe.bat."
                )
        except Exception as e:
            st.error(f"Erro ao carregar GUIA_EXECUTAVEL.md: {e}")

# ==========================================
# ==========================================
# SEÃ‡ÃƒO: PRÃ“XIMOS PASSOS
# ==========================================
elif indice_selecionado == "ðŸš€ PrÃ³ximos Passos":
    st.header("ðŸš€ PrÃ³ximos Passos â€” TC Copilot")

    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; margin-bottom: 20px;">
        <h2 style="color: white; margin: 0;">ðŸš€ PrÃ³ximos Passos â€” TC Copilot</h2>
        <p style="color: rgba(255,255,255,0.9); margin-top: 8px;">VisÃ£o completa, escopo funcional, plano tÃ©cnico e roadmap</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    Este documento apresenta a **visÃ£o completa**, o **escopo funcional**, o **plano tÃ©cnico**,
    o **processo corporativo** para obter acesso Ã  API de IA da Stellantis, e o **roadmap** necessÃ¡rio
    para desenvolver o **Agente de InteligÃªncia Artificial do TC VeÃ­culos** â€” uma evoluÃ§Ã£o estratÃ©gica
    que permitirÃ¡ anÃ¡lises automÃ¡ticas, resumos diÃ¡rios, identificaÃ§Ã£o de variaÃ§Ãµes e comentÃ¡rios
    inteligentes sobre o desempenho das oficinas e dos modelos.
    """)

    # ------------------------------------------------------------------
    # 0) Como conseguir acesso Ã  API de LLM da Stellantis (GENAI Gateway)
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 0) Como conseguir acesso Ã  API de LLM da Stellantis (GENAI Gateway)", expanded=False):
        st.markdown("""
        Antes de iniciar o desenvolvimento do agente, Ã© **obrigatÃ³rio** seguir o processo corporativo
        da Stellantis para obter acesso Ã  API oficial **GENAI Gateway**, que conecta os modelos de
        LLMs usados internamente (GPT, Llama, Mistral, Cohere etc.).

        ---

        #### 0.1 â€“ O que Ã© o GENAI Gateway

        A Stellantis disponibiliza uma plataforma corporativa de IA generativa chamada:

        âœ”ï¸ **GENAI Platform / GENAI Gateway**

        Ela oferece:
        - Acesso seguro a modelos LLM empresariais
        - Suporte a **GPTâ€‘4**, **Llama 3**, **Mistral**, **Cohere**, **Bedrock**, **Azure OpenAI**
        - FunÃ§Ãµes de:
          - Embeddings
          - VetorizaÃ§Ãµes
          - Vector Store (OpenSearch)
          - CriaÃ§Ã£o de workspaces
          - Upload de documentos da Ã¡rea
          - Agentes customizados internos

        Toda comunicaÃ§Ã£o Ã© feita via API corporativa utilizando:
        - **OAuth2** (PingFederate)
        - **mTLS** (certificado digital cliente)
        - **GraphQL**

        > ðŸ”’ Esta Ã© a **Ãºnica forma segura e aprovada** de usar LLMs dentro da Stellantis.

        ---

        #### 0.2 â€“ Passo a Passo Oficial para Obter Acesso

        **Passo 1 â€” Submeter o caso de uso no Brightidea (AI Use Case Factory)**

        Registrar a iniciativa no portal corporativo informando:
        - DescriÃ§Ã£o do problema
        - Caso de uso
        - Valor financeiro estimado
        - Impacto esperado
        - Unidade envolvida
        - BenefÃ­cios gerados

        **Passo 2 â€” AvaliaÃ§Ã£o pelo GenAI Ambassador**

        ApÃ³s o envio no Brightidea, um GenAI Ambassador farÃ¡:
        - AvaliaÃ§Ã£o tÃ©cnica inicial
        - ValidaÃ§Ã£o do alinhamento estratÃ©gico
        - AnÃ¡lise de riscos
        - Triagem de viabilidade de implementaÃ§Ã£o

        **Passo 3 â€” Passar pelo EA Gate (Enterprise Architecture Gate)**

        Existem duas aprovaÃ§Ãµes possÃ­veis:
        - **EA Gate 1** â†’ libera uso da API para testes / POC
        - **EA Gate 2** â†’ libera uso em produÃ§Ã£o

        A arquitetura revisa: seguranÃ§a, alinhamento com polÃ­ticas corporativas, aderÃªncia Ã  plataforma GENAI, impacto em dados e estruturas internas.

        **Passo 4 â€” Solicitar credenciais PingFederate (OAuth2)**

        A API exige: `client_id`, `client_secret`, endpoint do PingFederate.
        Sem isso, nenhuma chamada Ã  API serÃ¡ aceita.

        **Passo 5 â€” Solicitar certificado mTLS (dupla autenticaÃ§Ã£o)**

        Ã‰ necessÃ¡rio gerar:
        - Certificado digital cliente
        - Chave privada
        - Registro no CMP da Stellantis

        Esse certificado deve ser enviado em todas as requisiÃ§Ãµes API junto com o token OAuth2.

        **Passo 6 â€” (Se necessÃ¡rio) solicitar conta cloud corporativa**

        Dependendo da complexidade, pode ser solicitado pela TI uma conta AWS/Azure para:
        - Hospedar seu agente
        - Armazenar documentos corporativos
        - Gerenciar o vector store

        **Passo 7 â€” Testar no GENAI Playground e StellAI Lab**

        Antes da integraÃ§Ã£o:
        - **GENAI Playground** â†’ testes de prompts
        - **StellAI Lab** â†’ testes avanÃ§ados e prototipagem de agentes

        **Passo 8 â€” Integrar o TC VeÃ­culos ao GENAI Gateway**

        ApÃ³s aprovaÃ§Ã£o e credenciais liberadas:
        - Conectar ao endpoint GraphQL
        - Configurar tokens e certificados
        - Acionar modelos de IA
        - Criar embeddings e vector store
        - Integrar com parquets e tabelas do TC VeÃ­culos
        """)

    # ------------------------------------------------------------------
    # 1) VisÃ£o Geral do Agente de IA
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 1) VisÃ£o Geral do Agente de IA", expanded=False):
        st.markdown("""
        O **TC Copilot** serÃ¡ uma camada inteligente dentro do projeto, responsÃ¡vel por:

        - Responder perguntas sobre dados do TC VeÃ­culos
        - Gerar resumos diÃ¡rios automÃ¡ticos
        - Identificar as maiores variaÃ§Ãµes por oficina e modelo
        - Apontar deltas relevantes e tendÃªncias
        - Analisar **Budget Ã— Real Ã— Best Estimate (BE)**
        - Explicar desvios de FP, FA, CPU, Flex Budget e Rateios
        - Detectar anomalias de custo
        - Gerar comentÃ¡rios e insights automÃ¡ticos para diretoria

        O agente serÃ¡ capaz de entender tanto perguntas simples quanto anÃ¡lises profundas.

        > **Exemplo:** *"Explique os principais impactos do mÃªs e destaque qual oficina teve o maior desvio."*
        """)

    # ------------------------------------------------------------------
    # 2) O que o Agente serÃ¡ capaz de fazer
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 2) O que o Agente serÃ¡ capaz de fazer", expanded=False):
        st.markdown("""
        #### âœ” Perguntas sobre dados
        - Qual oficina teve maior aumento no FP?
        - Qual modelo apresentou maior CPU?
        - Onde aconteceu o maior desvio do Real Ã— Budget?

        #### âœ” Resumos automÃ¡ticos
        - Resumo diÃ¡rio consolidado
        - Resumo semanal de performance
        - ComentÃ¡rio executivo do mÃªs

        #### âœ” Insights automÃ¡ticos
        - IdentificaÃ§Ã£o de anomalias
        - TendÃªncias por oficina
        - Comportamento por modelo
        - Drivers principais de aumento de custo

        #### âœ” Suporte operacional
        - ComparaÃ§Ã£o entre plantas
        - ExplicaÃ§Ãµes de rateio
        - AnÃ¡lises de volume Ã— custo
        """)

    # ------------------------------------------------------------------
    # 3) Como o Agente irÃ¡ funcionar tecnicamente
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 3) Como o Agente irÃ¡ funcionar tecnicamente", expanded=False):
        st.markdown("""
        O agente serÃ¡ composto por **trÃªs camadas**:

        #### 3.1 â€“ Base de Conhecimento
        Alimentada com:
        - Parquets do TC VeÃ­culos e TC Ext
        - Tabelas consolidadas de FA, FP, CPU
        - BE, Budget, Real
        - Flex Budget
        - Tabelas de debug
        - Dados por oficina e modelo

        Esses dados serÃ£o indexados no **vector store** da plataforma GENAI.

        #### 3.2 â€“ LLM Corporativo Stellantis
        O agente irÃ¡ usar:
        - GPTâ€‘4 corporativo
        - Llama 3
        - Mistral
        - Cohere
        - Ou qualquer modelo disponibilizado

        AtravÃ©s do **GENAI Gateway**.

        #### 3.3 â€“ Camada de raciocÃ­nio
        O agente executarÃ¡:
        1. Recebe a pergunta
        2. Entende qual dado procurar
        3. Busca no vector store
        4. Faz cÃ¡lculos (CPU, FP, FA, BE etc)
        5. Gera resposta estruturada
        """)

    # ------------------------------------------------------------------
    # 4) Checklist para acessar a API GENAI
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 4) Checklist para acessar a API GENAI", expanded=False):
        st.markdown("""
        - [ ] Enviar caso no Brightidea
        - [ ] Passar pela avaliaÃ§Ã£o do GenAI Ambassador
        - [ ] Ser aprovado no EA Gate
        - [ ] Solicitar credenciais PingFederate
        - [ ] Solicitar certificado mTLS
        - [ ] Integrar com GENAI Playground
        - [ ] Criar account cloud se necessÃ¡rio
        - [ ] Construir workspace e knowledge base
        - [ ] Conectar o sistema ao GENAI Gateway
        """)

    # ------------------------------------------------------------------
    # 5) Diagrama de Alto NÃ­vel
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 5) Diagrama de Alto NÃ­vel", expanded=False):
        st.markdown("""
        ```
        UsuÃ¡rio
           â†“
        SCI (Stellantis Cost Intelligence) â€“ Pergunta
           â†“
        TC Copilot (Agente de IA)
           â†“
        GENAI Gateway â€“ LLM Corporativo
           â†“
        Vector Store + Embeddings (parquets do TC)
           â†“
        RaciocÃ­nio do Agente
           â†“
        Resposta Inteligente
        ```
        """)

    # ------------------------------------------------------------------
    # 6) Roadmap de ImplementaÃ§Ã£o
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 6) Roadmap de ImplementaÃ§Ã£o", expanded=False):
        st.markdown("""
        #### Fase 1 â€“ PreparaÃ§Ã£o dos dados
        - Consolidar parquets
        - Organizar base de conhecimento
        - Documentar variÃ¡veis e mÃ©tricas

        #### Fase 2 â€“ IntegraÃ§Ã£o GENAI
        - Obter credenciais
        - Criar chamada bÃ¡sica via GraphQL
        - Criar embeddings da base interna

        #### Fase 3 â€“ ConstruÃ§Ã£o das habilidades
        - Perguntas operacionais
        - Resumos automÃ¡ticos
        - AnÃ¡lise por oficina
        - IdentificaÃ§Ã£o de anomalias

        #### Fase 4 â€“ ProduÃ§Ã£o
        - Teste interno
        - ValidaÃ§Ã£o com diretoria
        - Logs e auditoria
        - PublicaÃ§Ã£o final
        """)

    # ------------------------------------------------------------------
    # 7) Exemplos de perguntas
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 7) Exemplos de perguntas que o Agente poderÃ¡ responder", expanded=False):
        st.markdown("""
        - *"Resumo diÃ¡rio do Real Ã— Budget."*
        - *"Quem puxou o delta de FP da oficina BS?"*
        - *"Qual modelo teve maior CPU no mÃªs?"*
        - *"FaÃ§a um comentÃ¡rio executivo do mÃªs."*
        - *"Mostre as oficinas com maior variaÃ§Ã£o de FA."*
        """)

    # ------------------------------------------------------------------
    # 8) ConsideraÃ§Ãµes de SeguranÃ§a
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 8) ConsideraÃ§Ãµes de SeguranÃ§a", expanded=False):
        st.markdown("""
        - ðŸ”’ Nenhum dado sai da Stellantis
        - ðŸ” Toda comunicaÃ§Ã£o usa **mTLS + PingFederate**
        - âœ… A API GENAI Ã© homologada pela TI
        - ðŸ“‚ O agente sÃ³ acessa dados internos do TC
        - ðŸ“‹ Logs de auditoria sÃ£o mantidos
        """)

    # ------------------------------------------------------------------
    # 9) ConclusÃ£o Executiva
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 9) ConclusÃ£o Executiva", expanded=False):
        st.markdown("""
        O **TC Copilot** Ã© um avanÃ§o estratÃ©gico que:

        - âœ… Aumenta a eficiÃªncia do time
        - âœ… Reduz retrabalhos tÃ©cnicos
        - âœ… Acelera anÃ¡lises complexas
        - âœ… Melhora a qualidade das explicaÃ§Ãµes executivas
        - âœ… Fortalece governanÃ§a e transparÃªncia
        - âœ… Suporta tomadas de decisÃ£o crÃ­ticas

        O projeto estÃ¡ alinhado com a **estratÃ©gia global de IA da Stellantis** e utiliza as
        tecnologias oficiais aprovadas, garantindo **seguranÃ§a, escalabilidade e compliance**.
        """)

    # ------------------------------------------------------------------
    # 10) Preenchimento do formulÃ¡rio Brightidea
    # ------------------------------------------------------------------
    with st.expander("ðŸŸ¦ 10) Preenchimento do FormulÃ¡rio Brightidea (AI Use Case Factory)", expanded=False):
        st.markdown("""
        FormulÃ¡rio oficial:

        ðŸ‘‰ [https://stellantis.brightidea.com/AIUseCaseFactory](https://stellantis.brightidea.com/AIUseCaseFactory)

        **RESPOSTAS COMPLETAS E PRONTAS PARA O BRIGHTIDEA (AI USE CASE FACTORY)**

        **Caso de Uso:** Stellantis Cost Intelligence (SCI â€” plataforma interna de inteligÃªncia de custos)

        **Slogan:** A evoluÃ§Ã£o da controladoria industrial

        ---

        #### ðŸ“Œ 1) DescriÃ§Ã£o do caso de uso (funÃ§Ã£o do usuÃ¡rio, problema, contexto)
        **FunÃ§Ã£o do principal usuÃ¡rio:**
        Analistas, especialistas e gestores de Controladoria Industrial, Controlling, Custos de Manufatura, FP&A (Financial Planning & Analysis â€” Planejamento e AnÃ¡lise Financeira), e equipes de performance fabril das plantas da Stellantis.

        **Problema a ser resolvido:**
        Hoje, anÃ¡lises de custo industrial (FP (Fluxo Principal), FA (Fluxo Auxiliar), Redis (receitas internas/redistribuiÃ§Ãµes do processo), CPU (Custo Por Unidade), Budget (orÃ§ado), Real (realizado), BE (Best Estimate â€” melhor estimativa/forecast) e Flex (Flex Budget â€” orÃ§amento flexÃ­vel)) exigem muito esforÃ§o manual para consolidaÃ§Ã£o, interpretaÃ§Ã£o, validaÃ§Ã£o e elaboraÃ§Ã£o de comentÃ¡rios executivos. Isso atrasa a tomada de decisÃ£o, gera retrabalho e produz inconsistÃªncias entre plantas.

        O sistema Stellantis Cost Intelligence (SCI) jÃ¡ automatiza cÃ¡lculos e consolida dados (mÃ³dulo TC VeÃ­culos), mas nÃ£o interpreta os resultados. A equipe precisa diariamente:
        - analisar desvios
        - identificar impactos
        - gerar resumos
        - explicar variaÃ§Ãµes por oficina
        - comentar principais movimentos
        - detectar comportamentos anÃ´malos

        Tudo isso ainda Ã© manual.

        **Contexto de negÃ³cios:**
        O projeto Stellantis Cost Intelligence (SCI â€” plataforma interna de inteligÃªncia de custos) propÃµe criar um Agente de IA (InteligÃªncia Artificial) integrado ao TC VeÃ­culos, usando o GENAI Gateway (Gateway corporativo de IA Generativa), capaz de interpretar dados internos automaticamente e fornecer:
        - anÃ¡lises instantÃ¢neas
        - explicaÃ§Ãµes sobre variaÃ§Ãµes
        - comentÃ¡rios executivos
        - identificaÃ§Ã£o automÃ¡tica de anomalias
        - insights sobre oficinas/modelos mais crÃ­ticos
        - resumos diÃ¡rios de performance
        - suporte Ã  gestÃ£o industrial e financeira

        Isso reduz retrabalho, padroniza anÃ¡lises e aumenta velocidade na tomada de decisÃ£o.

        #### ðŸ“Œ 2) Categoria de Recursos de IA
        **SeleÃ§Ã£o:** Busca de Conhecimento â€“ RecuperaÃ§Ã£o, AdaptaÃ§Ã£o e ReformulaÃ§Ã£o de InformaÃ§Ãµes

        **Motivo:** o agente analisarÃ¡ bases internas (parquets, tabelas, cÃ¡lculos do TC (Transformation Cost â€” custo de transformaÃ§Ã£o)) e gerarÃ¡ anÃ¡lises contextualizadas.
        TambÃ©m envolve interpretaÃ§Ã£o de dados estruturados â†’ mas a funÃ§Ã£o principal Ã© entender e explicar, nÃ£o prever.

        #### ðŸ“Œ 3) DomÃ­nio de NegÃ³cios
        **SeleÃ§Ã£o:** Manufacturing / Industrial Finance / Controladoria Industrial

        #### ðŸ“Œ 4) Escala regional do caso de uso
        **SeleÃ§Ã£o:** MÃºltiplas regiÃµes (AmÃ©rica do Sul e outras regiÃµes futuramente).
        O processo de controladoria industrial Ã© similar entre plantas LATAM (Latin America â€” AmÃ©rica Latina), podendo escalar para EU (Europe â€” Europa) e NA (North America â€” AmÃ©rica do Norte) facilmente.

        #### ðŸ“Œ 5) Marcas que podem se beneficiar
        **SeleÃ§Ã£o:** Todas as marcas Stellantis.
        Processo de custo fabril Ã© transversal (Fiat, Peugeot, CitroÃ«n, Jeep, RAM, etc).

        #### ðŸ“Œ 6) ReduÃ§Ã£o anual estimada de custos
        **SeleÃ§Ã£o:** 100â€“500 mil â‚¬/ano

        **Justificativa prÃ¡tica:**
        - reduÃ§Ã£o do tempo de anÃ¡lise manual
        - eliminaÃ§Ã£o de retrabalho
        - padronizaÃ§Ã£o de explicaÃ§Ãµes
        - velocidade de diagnÃ³stico
        - apoio direto Ã  tomada de decisÃ£o fabril

        #### ðŸ“Œ 7) Receita anual estimada
        **SeleÃ§Ã£o:** NÃ£o aplicÃ¡vel

        #### ðŸ“Œ 8) Pessoas impactadas
        **SeleÃ§Ã£o:** 50 a 100 usuÃ¡rios

        Inclui:
        - times de controladoria fabril
        - times de custos
        - FP&A
        - gestores de performance industrial
        - diretoria de manufatura
        - controllers regionais

        #### ðŸ“Œ 9) Como isso cria valor / modelo de negÃ³cio
        O SCI cria um mecanismo contÃ­nuo de geraÃ§Ã£o de valor, pois:
        - substitui anÃ¡lises manuais repetitivas
        - reduz tempo de elaboraÃ§Ã£o de comentÃ¡rios executivos
        - detecta problemas antecipadamente
        - evita inconsistÃªncias entre plantas
        - amplia governanÃ§a e padronizaÃ§Ã£o
        - acelera a tomada de decisÃ£o
        - facilita comparaÃ§Ãµes entre modelos e oficinas
        - disponibiliza inteligÃªncia financeira 24/7

        **MediÃ§Ãµes claras de valor:**
        - horas de retrabalho eliminadas
        - velocidade para fechar custos diÃ¡rios/mensais
        - quantidade de anÃ¡lises automatizadas
        - nÃºmero de alertas antecipados por anomalias detectadas
        - produtividade do time de controladoria

        #### ðŸ“Œ 10) Disponibilidade dos dados
        **SeleÃ§Ã£o:** Tenho muitos dados de boa qualidade prontos para uso.

        **Justificativa:**
        O sistema Stellantis Cost Intelligence (SCI) jÃ¡ contÃ©m:
        - parquets consolidados
        - tabelas tratadas (FP, FA, Redis, CPU, BE, Flex)
        - dados padronizados por oficina e modelo
        - tabelas auxiliares (debug, massa, rateios, percentuais)
        - banco de dados estruturado em Python

        Tudo jÃ¡ estÃ¡ higienizado e pronto para indexaÃ§Ã£o via GENAI (IA Generativa).

        #### ðŸ“Œ 11) Tipo de dados
        **Selecione TODOS os aplicÃ¡veis:**
        - âœ” Texto plano (CSV, parquet, etc.)
        - âœ” Documentos (documentaÃ§Ã£o tÃ©cnica do sistema, PDF, notas internas)
        - âœ” Dados estruturados (tabelas, parquets, bancos internos)

        #### ðŸ“Œ 12) Qualidade dos dados
        **SeleÃ§Ã£o:** Precisa, consistente e confiÃ¡vel para tomada de decisÃ£o.

        **Motivo:** O processo do TC VeÃ­culos jÃ¡ foi padronizado e validado internamente.

        #### ðŸ“Œ 13) Recursos / expertise necessÃ¡rios
        - Engenheiros de dados (para ingestÃ£o inicial do vector store (base vetorial))
        - Suporte GENAI COE (Center of Excellence â€” Centro de ExcelÃªncia de IA Generativa) (para mTLS (mutual TLS â€” TLS mÃºtuo) + OAuth2 (OAuth 2.0 â€” protocolo de autorizaÃ§Ã£o) via PingFederate)
        - Desenvolvedor Python (integraÃ§Ã£o TC (Transformation Cost) Ã— GENAI (IA Generativa))
        - Especialista de controladoria (validaÃ§Ã£o dos insights)

        #### ðŸ“Œ 14) SoluÃ§Ãµes concorrentes
        NÃ£o existe soluÃ§Ã£o semelhante dentro da Stellantis.
        Processos atuais sÃ£o manuais e fragmentados.

        #### ðŸ“Œ 15) Prazos desejados
        POC (Proof of Concept â€” prova de conceito) apÃ³s aprovaÃ§Ã£o â€” sem data rÃ­gida.
        Pode acompanhar calendÃ¡rio de FECHAMENTO MENSAL e BE.

        #### ðŸ“Œ 16) Tags
        cost-control, industrial-finance, manufacturing, genai, tc-veiculos, scicontroller, insights

        #### ðŸ“Œ 17) Patrocinador de NegÃ³cio
        Seu gestor ou diretor da Ã¡rea de Controlling/Manufatura (preencher com o nome interno).

        #### ðŸ“Œ 18) LÃ­der de TIC (se conhecido)
        Colocar o responsÃ¡vel de TI/IS (Tecnologia da InformaÃ§Ã£o / Information Systems â€” Sistemas de InformaÃ§Ã£o) local da planta ou regiÃ£o.

        #### ðŸ“Œ 19) JÃ¡ houve alguma aÃ§Ã£o?
        Sim â€” desenvolvimento do Stellantis Cost Intelligence (SCI) (mÃ³dulo TC VeÃ­culos), consolidaÃ§Ã£o dos dados, definiÃ§Ã£o do caso de uso e preparaÃ§Ã£o para integraÃ§Ã£o ao GENAI Gateway.

        #### ðŸ“Œ 20) LLM alvo
        GPTâ€‘4 / GPTâ€‘5.2 (Azure OpenAI via GENAI Gateway (Gateway corporativo de IA Generativa))

        #### ðŸ“Œ 21) Plataforma alvo
        Azure (Microsoft)

        Melhor integraÃ§Ã£o com Python, Streamlit e GENAI Gateway.
        """)

# RodapÃ©
st.markdown("---")
mes_atual = obter_mes_atual()
ano_atual = datetime.now().year
versao_atual = obter_versao_atual()
st.markdown(f"""
<div style='text-align: center; color: #666; padding: 20px;'>
    ðŸ“š Stellantis Cost Intelligence (SCI) | VersÃ£o {versao_atual} | {mes_atual} {ano_atual}
    <br>
    <small>Desenvolvido por Hudson Cardin, Lauro Paiva e Frederico Cesar de Jesus</small>
</div>
""", unsafe_allow_html=True)

