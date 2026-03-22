"""
TC Copilot — Página Streamlit principal.

Três abas:
  1. 💬 Chatbot — consulta inteligente ao vivo (dados parquet)
  2. 📄 Relatório — gerar relatório PDF e exibir na tela
     2a. Relatório Automático (sem API)
      2b. Relatório com IA (provider configurado)
     2c. Biblioteca de PDFs — visualizar/baixar todos os PDFs gerados
  3. ⚙️ Configuração — provider, credenciais, modelo, idioma
"""

from __future__ import annotations

import os
import time as _time
import streamlit as st

from tc_copilot.config import (
    IDIOMAS,
    MODELOS_DATABRICKS,
    MODELOS_LLM,
    PASTA_RELATORIOS,
    carregar_api_key,
    carregar_databricks_cfg,
    caminho_downloads_usuario,
    carregar_idioma,
    carregar_modelo,
    carregar_provider,
    caminho_relatorio,
    em_execucao_empacotada,
    salvar_bytes_em_downloads,
    salvar_api_key,
    salvar_databricks_cfg,
    carregar_copilot_habilitado,
    salvar_idioma,
    salvar_modelo,
    salvar_provider,
    salvar_copilot_habilitado,
)

import re as _re
from datetime import datetime as _datetime

# ═══════════════════════════════════════════════════════════════
#  INFERÊNCIA DE MÊS A PARTIR DA PERGUNTA
# ═══════════════════════════════════════════════════════════════

_MESES_MAP: dict[str, int] = {
    # pt-BR
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
    # abreviações pt-BR
    "jan": 1, "fev": 2, "mar": 3, "abr": 4,
    "mai": 5, "jun": 6, "jul": 7, "ago": 8,
    "set": 9, "out": 10, "nov": 11, "dez": 12,
    # en
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    # abreviações en
    "feb": 2, "apr": 4, "aug": 8, "sep": 9, "oct": 10, "dec": 12,
}


def _render_pdf_download_control(
    pdf_path: str,
    file_name: str,
    label: str,
    key: str,
    *,
    use_container_width: bool = True,
) -> None:
    """Salva em Downloads no EXE e mantém download normal no navegador."""
    with open(pdf_path, "rb") as f:
        payload = f.read()

    if em_execucao_empacotada():
        if st.button(
            label,
            key=f"{key}_downloads",
            use_container_width=use_container_width,
        ):
            try:
                destino = salvar_bytes_em_downloads(file_name, payload)
                st.success(f"✅ Arquivo salvo em: {destino}")
                st.info(
                    "📁 Verifique sua pasta Downloads: "
                    f"{caminho_downloads_usuario()}"
                )
            except Exception as exc:
                st.error(f"❌ Falha ao salvar em Downloads: {exc}")
        return

    st.download_button(
        label=label,
        data=payload,
        file_name=file_name,
        mime="application/pdf",
        use_container_width=use_container_width,
        key=key,
    )


def _inferir_mes_da_pergunta(pergunta: str, meses_disponiveis: list[int]) -> int:
    """Extrai o mês mencionado na pergunta do usuário (regex local, sem LLM).

    Se nenhum mês for encontrado, retorna o último mês disponível.
    """
    texto = pergunta.lower().strip()
    for nome, num in sorted(_MESES_MAP.items(), key=lambda x: -len(x[0])):
        # Buscar a palavra inteira (word boundary)
        if _re.search(rf"\b{_re.escape(nome)}\b", texto):
            if num in meses_disponiveis:
                return num
            # Mes mencionado mas sem dados — fallback
            break
    # Fallback: último mês disponível
    return meses_disponiveis[-1] if meses_disponiveis else 1


# ═══════════════════════════════════════════════════════════════
#  INFERÊNCIA DE ANO A PARTIR DA PERGUNTA
# ═══════════════════════════════════════════════════════════════

_ANO_ANTERIOR_KEYWORDS = [
    "ano passado", "ano anterior", "last year", "previous year",
    "año pasado", "año anterior",
]


def _inferir_ano_da_pergunta(pergunta: str, anos_disponiveis: list[int]) -> int:
    """Extrai o ano mencionado na pergunta (regex local, sem LLM).

    Detecta:
      - 4 dígitos no range 2020-2039 (ex: "2025", "2026")
      - "ano passado", "ano anterior", "last year"
    Se não encontrar, retorna o primeiro ano da lista (mais recente).
    """
    texto = pergunta.lower().strip()

    # Checar keywords de "ano anterior"
    for kw in _ANO_ANTERIOR_KEYWORDS:
        if kw in texto:
            ano_ant = _datetime.now().year - 1
            if ano_ant in anos_disponiveis:
                return ano_ant
            break

    # Checar ano explícito (4 dígitos, range 2020-2039)
    match = _re.search(r"\b(20[2-3]\d)\b", texto)
    if match:
        ano = int(match.group(1))
        if ano in anos_disponiveis:
            return ano

    # Fallback: primeiro ano (mais recente)
    return anos_disponiveis[0] if anos_disponiveis else _datetime.now().year


# ═══════════════════════════════════════════════════════════════
#  INFERÊNCIA DE PERÍODO (MULTI-MÊS)
# ═══════════════════════════════════════════════════════════════

_TRIMESTRES = {
    "q1": [1, 2, 3], "q2": [4, 5, 6], "q3": [7, 8, 9], "q4": [10, 11, 12],
    "1º trimestre": [1, 2, 3], "2º trimestre": [4, 5, 6],
    "3º trimestre": [7, 8, 9], "4º trimestre": [10, 11, 12],
    "primeiro trimestre": [1, 2, 3], "segundo trimestre": [4, 5, 6],
    "terceiro trimestre": [7, 8, 9], "quarto trimestre": [10, 11, 12],
    "1st quarter": [1, 2, 3], "2nd quarter": [4, 5, 6],
    "3rd quarter": [7, 8, 9], "4th quarter": [10, 11, 12],
}

_SEMESTRES = {
    "1º semestre": list(range(1, 7)), "2º semestre": list(range(7, 13)),
    "primeiro semestre": list(range(1, 7)), "segundo semestre": list(range(7, 13)),
    "1st half": list(range(1, 7)), "2nd half": list(range(7, 13)),
}

# Mapa inverso nome→número (reusar _MESES_MAP)
_MESES_NOME_PARA_NUM = _MESES_MAP  # alias


def _inferir_periodo_da_pergunta(
    pergunta: str, meses_disponiveis: list[int],
) -> list[int]:
    """Detecta um ou mais meses na pergunta do usuário.

    Padrões reconhecidos (em ordem de prioridade):
      1. YTD / acumulado
      2. Trimestres (Q1, Q2, "1º trimestre")
      3. Semestres ("1º semestre")
      4. Range: "janeiro a março", "de jan a mar"
      5. Enumeração: "janeiro e fevereiro", "jan, fev e mar"
      6. "últimos N meses"
      7. Mês único (fallback para lista unitária)

    Retorna lista de meses (int) filtrados contra meses_disponiveis.
    Se nada for encontrado, retorna [último_mês_disponível].
    """
    texto = pergunta.lower().strip()

    # 1) YTD / acumulado / "ano inteiro"
    if _re.search(r"\b(ytd|acumulado|year[ -]to[ -]date|ano inteiro|ano completo|full year)\b", texto):
        resultado = [m for m in meses_disponiveis]
        if resultado:
            return resultado

    # 2) Trimestres
    for label, meses in _TRIMESTRES.items():
        if label in texto:
            resultado = [m for m in meses if m in meses_disponiveis]
            if resultado:
                return resultado

    # 3) Semestres
    for label, meses in _SEMESTRES.items():
        if label in texto:
            resultado = [m for m in meses if m in meses_disponiveis]
            if resultado:
                return resultado

    # 4) "últimos N meses"
    match_ultimos = _re.search(
        r"\b[úu]ltimos?\s+(\d+)\s+m[eê]s(?:es)?\b", texto,
    )
    if match_ultimos:
        n = int(match_ultimos.group(1))
        if meses_disponiveis and n > 0:
            return meses_disponiveis[-n:]

    # 5) Range: "janeiro a março", "de jan a mar", "jan-mar"
    nomes_sorted = sorted(_MESES_MAP.items(), key=lambda x: -len(x[0]))
    nomes_pattern = "|".join(_re.escape(n) for n, _ in nomes_sorted)
    match_range = _re.search(
        rf"\b({nomes_pattern})\s*(?:a|até|to|[-–])\s*({nomes_pattern})\b",
        texto,
    )
    if match_range:
        m_inicio = _MESES_MAP.get(match_range.group(1))
        m_fim = _MESES_MAP.get(match_range.group(2))
        if m_inicio and m_fim and m_inicio <= m_fim:
            resultado = [m for m in range(m_inicio, m_fim + 1) if m in meses_disponiveis]
            if resultado:
                return resultado

    # 6) Enumeração: "janeiro e fevereiro", "jan, fev e mar"
    meses_encontrados = []
    for nome, num in nomes_sorted:
        if _re.search(rf"\b{_re.escape(nome)}\b", texto):
            if num not in meses_encontrados and num in meses_disponiveis:
                meses_encontrados.append(num)
    if len(meses_encontrados) >= 2:
        return sorted(meses_encontrados)

    # 7) Mês único
    if len(meses_encontrados) == 1:
        return meses_encontrados

    # Fallback: último mês disponível
    if meses_disponiveis:
        return [meses_disponiveis[-1]]
    return [1]



def render():
    st.header("🤖 TC Copilot")
    st.caption("Assistente inteligente para análise de custos — TC Veículos")

    if not carregar_copilot_habilitado():
        st.warning(
            "⚠️ TC Copilot está desativado. "
            "Ative na aba **⚙️ Configuração** abaixo."
        )
        _render_configuracao()
        return

    tab_chat, tab_relatorio, tab_config = st.tabs([
        "💬 Chatbot",
        "📄 Relatório Veic.",
        "⚙️ Configuração",
    ])

    with tab_chat:
        @st.fragment
        def _render_tab_chat():
            _render_chatbot()

        _render_tab_chat()

    with tab_relatorio:
        @st.fragment
        def _render_tab_relatorio():
            _render_gerar_relatorio()

        _render_tab_relatorio()

    with tab_config:
        @st.fragment
        def _render_tab_config():
            _render_configuracao()


    # ═══════════════════════════════════════════════════════════════
    #  ABA 1 — CHATBOT (consulta ao vivo nos parquets)
    # ═══════════════════════════════════════════════════════════════

    def _render_chatbot():
        from tc_copilot.data_collector import (
            configurar_moeda_formatacao,
            descobrir_anos_disponiveis,
            descobrir_meses_disponiveis,
            formatar_contexto_parquet,
            formatar_contexto_parquet_periodo,
        )
        from tc_copilot.llm_integration import responder_consulta_live
        from tc_copilot.prompts import obter_nome_mes

        st.subheader("Consulta Inteligente")
        st.caption(
            "Pergunte diretamente sobre os dados de custos "
            "— acesso ao vivo nos parquets"
        )
        with st.expander("📖 Dimensões e colunas disponíveis", expanded=False):
            st.markdown(
                "| Nível | Exemplos | Como perguntar |\n"
                "| --- | --- | --- |\n"
                "| Type 05 | Labor, Burden, D&A | categorias macro de custo |\n"
                "| Type 06 | Benefits, Direct Labor, Energy, Expenses, "
                "Maintenance | subcategorias dentro do Type 05 |\n"
                "| Account | Main/Principal, Restaurant-BC, Health-BC, Gas, "
                "Water, Third Part Services / Uma | contas detalhadas dentro do "
                "Type 06 |\n"
                "| **Texto breve** | peças reposição, serviço elétrico, "
                "manutenção predial | **detalhe mais granular** dos gastos "
                "realizados dentro de cada Account |\n"
                "| Oficina | Prensas, Armação, Soldagem | onde o impacto "
                "aconteceu |"
            )
            st.caption(
                "Dica rápida: perguntas sobre 'manutenção', 'benefícios', "
                "'energia' ou 'serviços de terceiros' agora podem ser "
                "detalhadas até o nível Account e **Texto breve** "
                "(descrição do lançamento contábil)."
            )
            st.markdown(
                "**Exemplos de perguntas**\n"
                "- Quais foram os maiores gastos realizados em Maintenance "
                "este mês?\n"
                "- Top 5 Accounts com maior desvio vs Budget em Benefits.\n"
                "- Em Energy, quais contas mais pressionaram o Real vs Mês "
                "Anterior?\n"
                "- Quais oficinas tiveram maior impacto em Burden e qual "
                "Type 06 puxou isso?\n"
                "- **Quais são os maiores gastos do Account Third Part "
                "Services / Uma?** (detalha por Texto breve)\n"
                "- Detalhe os gastos de Maintenance por Account e Texto "
                "breve.\n"
                "- Quais foram os maiores ganhos e perdas em Third Part "
                "Services / Uma?"
            )

        provider = carregar_provider()
        api_key = carregar_api_key()
        databricks_cfg = carregar_databricks_cfg()
        llm_configurada = bool(api_key) if provider == "openai" else bool(
            databricks_cfg["url"]
            and databricks_cfg["endpoint"]
            and databricks_cfg["token"]
        )
        if not llm_configurada:
            if provider == "databricks_claude":
                st.error(
                    "❌ Configuração do Databricks necessária para consultas. "
                    "Preencha URL, endpoint e token na aba **⚙️ Configuração**."
                )
            else:
                st.error(
                    "❌ Chave da OpenAI necessária para consultas. "
                    "Configure na aba **⚙️ Configuração**."
                )
            return

        # ── Selecionar ano e mês ──
        anos = descobrir_anos_disponiveis()
        if not anos:
            st.warning("Nenhum dado disponível.")
            return

        col_ano, col_mes = st.columns(2)
        with col_ano:
            opcoes_anos_display = ["(inferir da pergunta)"] + [str(a) for a in anos]
            ano_label = st.selectbox("Ano", opcoes_anos_display, index=0, key="chat_ano")
            if ano_label == "(inferir da pergunta)":
                ano_numero = None
                # Usar primeiro ano (mais recente) para popular meses inicialmente
                ano_para_meses = anos[0]
            else:
                ano_numero = int(ano_label)
                ano_para_meses = ano_numero
        with col_mes:
            meses_disp = descobrir_meses_disponiveis(ano_para_meses)
            if not meses_disp:
                st.warning(f"Nenhum mês com dados para {ano_para_meses}.")
                return
            opcoes_meses = {obter_nome_mes(m, "pt-BR"): m for m in meses_disp}
            # Primeira opção vazia para inferir da pergunta
            opcoes_display = ["(inferir da pergunta)"] + list(opcoes_meses.keys())
            mes_label = st.selectbox(
                "Mês",
                opcoes_display,
                index=0,  # Default: inferir da pergunta
                key="chat_mes",
            )
            if mes_label == "(inferir da pergunta)":
                mes_numero = None
            else:
                mes_numero = opcoes_meses[mes_label]

        # ── Histórico de chat ──
        _chat_ano = ano_numero if ano_numero is not None else "auto"
        _chat_mes = mes_numero if mes_numero is not None else "auto"
        chat_key = f"copilot_chat_{_chat_ano}_{_chat_mes}"
        if chat_key not in st.session_state:
            st.session_state[chat_key] = []

        historico = st.session_state[chat_key]

        # Mostrar histórico
        for msg in historico:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"].replace("$", "\\$"))

        # ── Input do usuário ──
        _display_ano = ano_label if ano_numero else "(ano inferido)"
        _input_label = (
            f"Pergunte sobre {mes_label}/{_display_ano}..."
            if mes_numero
            else f"Pergunte sobre {_display_ano} (mês/período será inferido)..."
        )
        pergunta = st.chat_input(
            _input_label,
            key="copilot_chat_input",
        )

        if pergunta:
            # Adicionar pergunta
            historico.append({"role": "user", "content": pergunta})
            with st.chat_message("user"):
                st.markdown(pergunta)

            # Buscar contexto dos parquets e responder
            with st.chat_message("assistant"):
                with st.spinner("Consultando dados..."):
                    # --- Inferir ano da pergunta se não selecionado ---
                    _ano_efetivo = ano_numero if ano_numero is not None else _inferir_ano_da_pergunta(pergunta, anos)
                    _ano_inferido = ano_numero is None

                    # Recalcular meses disponíveis para o ano efetivo
                    if _ano_inferido:
                        _meses_disp_efetivo = descobrir_meses_disponiveis(_ano_efetivo)
                        if not _meses_disp_efetivo:
                            _meses_disp_efetivo = meses_disp
                            _ano_efetivo = ano_para_meses
                    else:
                        _meses_disp_efetivo = meses_disp

                    # --- Inferir período (mês ou multi-mês) ---
                    _periodo_inferido = False
                    if mes_numero is not None:
                        # Mês explícito selecionado
                        _meses_efetivos = [mes_numero]
                    else:
                        _meses_efetivos = _inferir_periodo_da_pergunta(pergunta, _meses_disp_efetivo)
                        _periodo_inferido = True

                    # Configurar moeda antes de formatar contexto
                    moeda = st.session_state.get("copilot_moeda", "EUR")
                    simbolo = st.session_state.get("copilot_simbolo", "€")
                    taxas = st.session_state.get(
                        "copilot_taxas",
                        {"BRL": 1.0, "USD": 0.20, "EUR": 1.0 / 6.4855},
                    )
                    taxa_conversao = taxas.get(moeda, 1.0)
                    configurar_moeda_formatacao(moeda, simbolo)

                    # --- Montar contexto (single-month ou multi-month) ---
                    if len(_meses_efetivos) == 1:
                        contexto = formatar_contexto_parquet(
                            _ano_efetivo, _meses_efetivos[0], taxa_conversao=taxa_conversao,
                        )
                    else:
                        contexto = formatar_contexto_parquet_periodo(
                            _ano_efetivo, _meses_efetivos, taxa_conversao=taxa_conversao,
                        )

                    # Prefixo informativo sobre inferência
                    if _periodo_inferido:
                        nomes_meses = [obter_nome_mes(m, "pt-BR") for m in _meses_efetivos]
                        if len(nomes_meses) == 1:
                            _info = f"⚠️ Mês inferido da pergunta: {nomes_meses[0]}/{_ano_efetivo}"
                        else:
                            _info = f"⚠️ Período inferido da pergunta: {nomes_meses[0]}–{nomes_meses[-1]}/{_ano_efetivo}"
                        if _ano_inferido:
                            _info += f" (ano inferido: {_ano_efetivo})"
                        contexto = _info + "\n\n" + contexto
                    elif _ano_inferido:
                        contexto = f"⚠️ Ano inferido da pergunta: {_ano_efetivo}\n\n" + contexto

                    modelo = st.session_state.get("copilot_modelo", "gpt-4o-mini")
                    idioma = st.session_state.get("copilot_idioma", "pt-BR")

                    resposta = responder_consulta_live(
                        pergunta=pergunta,
                        contexto_dados=contexto,
                        historico_chat=historico[:-1],  # Sem a pergunta atual
                        idioma=idioma,
                        api_key=api_key,
                        model=modelo,
                        moeda=moeda,
                    )

                    st.markdown(resposta.replace("$", "\\$"))

                    historico.append({"role": "assistant", "content": resposta})

        # ── Limpar histórico ──
        col_limpar, col_info = st.columns([1, 3])
        with col_limpar:
            if historico:
                if st.button("🗑️ Limpar conversa", key="limpar_chat"):
                    st.session_state[chat_key] = []
                    st.rerun()
        with col_info:
            if historico:
                st.caption(f"{len(historico)} mensagens na conversa")

        # ── Sugestões de perguntas ──
        if not historico:
            st.markdown("**💡 Sugestões de perguntas:**")
            sugestoes = [
                f"Quais foram os maiores desvios de Labor em {mes_label} de {ano_para_meses}?",
                f"Qual oficina teve maior impacto no custo em {mes_label}?",
                f"Compare Labor, Burden e D&A vs Flex Budget em {mes_label}",
                f"Quais modelos tiveram maior variação de volume vs mês anterior?",
                "Houve alguma anomalia significativa nos custos?",
                f"Quais os maiores gastos do Q1 {ano_para_meses}?",
            ]
            for sug in sugestoes:
                if st.button(f"💡 {sug}", key=f"sug_{hash(sug)}"):
                    st.session_state[chat_key].append({"role": "user", "content": sug})
                    st.rerun()


    # ═══════════════════════════════════════════════════════════════
    #  ABA 2 — GERAR RELATÓRIO
    # ═══════════════════════════════════════════════════════════════

    def _render_gerar_relatorio():
        """Renderiza a aba de relatório com sub-tabs: Automático, Com IA e Biblioteca."""
        st.subheader("Gerar Relatório Anual")

        sub_auto, sub_ia, sub_biblio = st.tabs([
            "📄 Relatório Automático",
            "🤖 Relatório com IA",
            "📁 Biblioteca de PDFs",
        ])

        with sub_auto:
            _render_relatorio_local()

        with sub_ia:
            _render_relatorio_ia()

        with sub_biblio:
            _render_biblioteca_pdfs()


    # ── SUB-TAB: RELATÓRIO AUTOMÁTICO (SEM API) ──

    def _render_relatorio_local():
        from tc_copilot.data_collector import (
            descobrir_anos_disponiveis,
            descobrir_meses_disponiveis,
        )
        from tc_copilot.report_generator import (
            carregar_dados_relatorio_local,
            gerar_relatorio_mes_local,
            meses_ja_gerados_local,
        )
        from tc_copilot.config import caminho_relatorio_local
        from tc_copilot.prompts import LABELS, obter_nome_mes

        st.info(
            "📄 Relatório gerado **sem API** — textos analíticos "
            "produzidos automaticamente por templates Python."
        )

        # ── Selecionar ano ──
        anos = descobrir_anos_disponiveis()
        if not anos:
            st.error("Nenhum ano com dados processados encontrado em dados/TC_Principal/")
            return

        ano = st.selectbox("Ano", anos, key="rel_local_ano")

        # ── Meses disponíveis vs já gerados ──
        meses_disp = descobrir_meses_disponiveis(ano)
        meses_gerados = meses_ja_gerados_local(ano)

        if not meses_disp:
            st.warning(f"Nenhum mês com dados Real encontrado para {ano}.")
            return

        # Mostrar status
        st.markdown("**Status dos meses:**")
        cols = st.columns(min(len(meses_disp), 6))
        for i, mes_num in enumerate(meses_disp):
            col = cols[i % len(cols)]
            nome = obter_nome_mes(mes_num, "pt-BR")[:3]
            status = "✅" if mes_num in meses_gerados else "⬜"
            col.markdown(f"{status} **{nome}**")

        st.divider()

        # ── Seletor de mês (multiselect) ──
        opcoes_meses = {
            f"{obter_nome_mes(m, 'pt-BR')} {'✅' if m in meses_gerados else ''}": m
            for m in meses_disp
        }
        meses_selecionados_labels = st.multiselect(
            "Mês a gerar/regenerar",
            list(opcoes_meses.keys()),
            default=None,
            key="rel_local_mes",
        )
        meses_selecionados = [opcoes_meses[lbl] for lbl in meses_selecionados_labels]

        gerar_todos = st.checkbox(
            "Gerar todos os meses disponíveis de uma vez",
            help="Gera capítulos para todos os meses com dados.",
            key="rel_local_todos",
        )

        idioma = st.session_state.get("copilot_idioma", "pt-BR")

        # ── Botão gerar ──
        col_btn1, col_btn2 = st.columns([1, 3])
        with col_btn1:
            btn_gerar = st.button(
                "🚀 Gerar Relatório",
                use_container_width=True,
                type="primary",
                key="btn_gerar_local",
            )

        if btn_gerar:
            meses_para_gerar = meses_disp if gerar_todos else meses_selecionados
            if not meses_para_gerar:
                st.warning("Selecione ao menos um mês para gerar.")
                st.stop()
            # Obter configuração de moeda da sessão
            _moeda = st.session_state.get("copilot_moeda", "EUR")
            _taxas = st.session_state.get("copilot_taxas", {})

            _t0_btn = _time.time()
            progress = st.progress(0, text="Iniciando geração...")
            total = len(meses_para_gerar)
            pdf_path = None

            for idx, mes_num in enumerate(meses_para_gerar):
                nome_mes = obter_nome_mes(mes_num, "pt-BR")
                progress.progress(
                    (idx) / total,
                    text=f"Gerando capítulo: {nome_mes} ({idx+1}/{total})...",
                )
                try:
                    pdf_path = gerar_relatorio_mes_local(
                        ano=ano,
                        mes_numero=mes_num,
                        idioma=idioma,
                        moeda=_moeda,
                        taxas=_taxas,
                        tempo_inicio=_t0_btn,
                    )
                except Exception as e:
                    st.error(f"Erro ao gerar {nome_mes}: {e}")
                    continue

            progress.progress(1.0, text="Concluído!")
            if pdf_path and os.path.exists(pdf_path):
                st.success("✅ Relatório gerado com sucesso!")
                st.balloons()

        # ── Download do PDF ──
        pdf_existente = str(caminho_relatorio_local(ano))
        if os.path.exists(pdf_existente):
            st.divider()
            tamanho_mb = os.path.getsize(pdf_existente) / (1024 * 1024)
            col1, col2, col3 = st.columns([1, 1, 2])
            col1.metric("Meses gerados", len(meses_gerados))
            col2.metric("Tamanho PDF", f"{tamanho_mb:.1f} MB")
            with col3:
                _render_pdf_download_control(
                    pdf_existente,
                    f"relatorio_tc_{ano}_local.pdf",
                    "📥 Baixar PDF Anual",
                    "dl_pdf_local",
                )

        # ── Downloads mensais individuais ──
        _render_downloads_mensais(ano, meses_gerados, modo="local")

        # ── Exibir relatório ──
        _render_resultado_relatorio(ano, idioma, modo="local")


    # ── SUB-TAB: RELATÓRIO COM IA ──

    def _render_relatorio_ia():
        from tc_copilot.data_collector import (
            descobrir_anos_disponiveis,
            descobrir_meses_disponiveis,
        )
        from tc_copilot.report_generator import (
            gerar_relatorio_mes,
            meses_ja_gerados,
        )
        from tc_copilot.prompts import obter_nome_mes

        provider = carregar_provider()
        api_key = carregar_api_key()
        databricks_cfg = carregar_databricks_cfg()
        llm_configurada = bool(api_key) if provider == "openai" else bool(
            databricks_cfg["url"]
            and databricks_cfg["endpoint"]
            and databricks_cfg["token"]
        )
        if not llm_configurada:
            mensagem = (
                "⚠️ Configuração do Databricks não encontrada. "
                if provider == "databricks_claude"
                else "⚠️ Chave da OpenAI não configurada. "
            )
            st.warning(
                mensagem
                + "O relatório será gerado sem análise inteligente "
                + "(dados brutos apenas). "
                + "Configure na aba **⚙️ Configuração**."
            )

        # ── Selecionar ano ──
        anos = descobrir_anos_disponiveis()
        if not anos:
            st.error("Nenhum ano com dados processados encontrado em dados/TC_Principal/")
            return

        ano = st.selectbox("Ano", anos, key="rel_ano", help="Selecione o ano do relatório")

        # ── Meses disponíveis vs já gerados ──
        meses_disp = descobrir_meses_disponiveis(ano)
        meses_gerados = meses_ja_gerados(ano)

        if not meses_disp:
            st.warning(f"Nenhum mês com dados Real encontrado para {ano}.")
            return

        # Mostrar status dos meses
        st.markdown("**Status dos meses:**")
        cols = st.columns(min(len(meses_disp), 6))
        for i, mes_num in enumerate(meses_disp):
            col = cols[i % len(cols)]
            nome = obter_nome_mes(mes_num, "pt-BR")[:3]
            status = "✅" if mes_num in meses_gerados else "⬜"
            col.markdown(f"{status} **{nome}**")

        st.divider()

        # ── Selecionar meses para gerar (multiselect) ──
        opcoes_meses = {
            f"{obter_nome_mes(m, 'pt-BR')} {'✅' if m in meses_gerados else ''}": m
            for m in meses_disp
        }
        meses_selecionados_labels = st.multiselect(
            "Mês a gerar/regenerar",
            list(opcoes_meses.keys()),
            default=None,
            key="rel_mes",
        )
        meses_selecionados = [opcoes_meses[lbl] for lbl in meses_selecionados_labels]

        # ── Opção de gerar múltiplos meses ──
        gerar_todos = st.checkbox(
            "Gerar todos os meses disponíveis de uma vez",
            help="Gera capítulos para todos os meses com dados. Pode demorar.",
        )

        # ── Configurações da geração ──
        modelo = st.session_state.get("copilot_modelo", "gpt-4o-mini")
        idioma = st.session_state.get("copilot_idioma", "pt-BR")

        st.info(f"Modelo: **{modelo}** | Idioma: **{IDIOMAS.get(idioma, idioma)}**")

        # ── Botão gerar ──
        col_btn1, col_btn2 = st.columns([1, 3])
        with col_btn1:
            btn_gerar = st.button(
                "🚀 Gerar Relatório",
                use_container_width=True,
                type="primary",
            )

        if btn_gerar:
            meses_para_gerar = meses_disp if gerar_todos else meses_selecionados
            if not meses_para_gerar:
                st.warning("Selecione ao menos um mês para gerar.")
                st.stop()

            progress = st.progress(0, text="Iniciando geração...")
            total = len(meses_para_gerar)
            pdf_path = None
            _t0_btn = _time.time()

            for idx, mes_num in enumerate(meses_para_gerar):
                nome_mes = obter_nome_mes(mes_num, "pt-BR")
                progress.progress(
                    (idx) / total,
                    text=f"Gerando capítulo: {nome_mes} ({idx+1}/{total})...",
                )

                try:
                    _moeda_ia = st.session_state.get("copilot_moeda", "EUR")
                    _taxas_ia = st.session_state.get("copilot_taxas", {})
                    pdf_path = gerar_relatorio_mes(
                        ano=ano,
                        mes_numero=mes_num,
                        api_key=api_key,
                        modelo=modelo,
                        idioma=idioma,
                        moeda=_moeda_ia,
                        taxas=_taxas_ia,
                        tempo_inicio=_t0_btn,
                    )
                except Exception as e:
                    st.error(f"Erro ao gerar {nome_mes}: {e}")
                    continue

            progress.progress(1.0, text="Concluído!")

            if pdf_path and os.path.exists(pdf_path):
                st.success("✅ Relatório gerado com sucesso!")
                st.balloons()

        # ── Download do PDF (se existir) ──
        pdf_existente = str(caminho_relatorio(ano))
        if os.path.exists(pdf_existente):
            st.divider()
            tamanho_mb = os.path.getsize(pdf_existente) / (1024 * 1024)
            col1, col2, col3 = st.columns([1, 1, 2])
            col1.metric("Meses gerados", len(meses_gerados))
            col2.metric("Tamanho PDF", f"{tamanho_mb:.1f} MB")
            with col3:
                _render_pdf_download_control(
                    pdf_existente,
                    f"relatorio_tc_{ano}.pdf",
                    "📥 Baixar PDF Anual",
                    "dl_pdf_ia",
                )

        # ── Downloads mensais individuais ──
        _render_downloads_mensais(ano, meses_gerados, modo="ia")

        # ── Exibir relatório ──
        _render_resultado_relatorio(ano, idioma, modo="ia")


    # ═══════════════════════════════════════════════════════════════
    #  BIBLIOTECA DE PDFs
    # ═══════════════════════════════════════════════════════════════

    def _render_regenerar_mensais_faltantes():
        """Verifica se há meses no JSON sem PDF mensal e oferece regeneração."""
        from tc_copilot.config import caminho_relatorio_mensal
        from tc_copilot.report_generator import (
            meses_ja_gerados_local,
            meses_ja_gerados,
        )

        faltantes: list[tuple[int, int, str]] = []  # (ano, mes, modo)

        for ano in range(2024, 2028):
            for modo, fn_meses in [
                ("local", meses_ja_gerados_local),
                ("ia", meses_ja_gerados),
            ]:
                try:
                    meses = fn_meses(ano)
                except Exception:
                    continue
                for m in meses:
                    pdf = str(caminho_relatorio_mensal(ano, m, modo))
                    if not os.path.exists(pdf):
                        faltantes.append((ano, m, modo))

        if not faltantes:
            return

        st.warning(
            f"⚠️ **{len(faltantes)}** PDF(s) mensal(is) "
            f"faltando para meses já gerados."
        )
        if st.button(
            "🔄 Gerar PDFs mensais faltantes",
            key="btn_regen_mensais",
        ):
            from tc_copilot.report_generator import gerar_pdf_mensal
            _t0_regen = _time.time()
            progress = st.progress(0, text="Gerando...")
            ok = 0
            for i, (ano, mes, modo) in enumerate(faltantes):
                progress.progress(
                    (i + 1) / len(faltantes),
                    text=f"Gerando {modo} {mes:02d}/{ano}...",
                )
                try:
                    r = gerar_pdf_mensal(ano, mes, modo=modo, tempo_inicio=_t0_regen)
                    if r:
                        ok += 1
                except Exception:
                    pass
            progress.progress(1.0, text="Concluído!")
            st.success(f"✅ {ok}/{len(faltantes)} PDFs gerados.")
            st.rerun()


    def _render_biblioteca_pdfs():
        """Lista todos os PDFs da pasta documentacao_anual/ com download."""
        import re
        from datetime import datetime
        from pathlib import Path

        st.info(
            "📁 Todos os relatórios PDF gerados ficam na pasta "
            "`documentacao_anual/`. Aqui você pode visualizar e baixar qualquer um."
        )
        if em_execucao_empacotada():
            st.caption(
                "No executável, os botões de PDF salvam uma cópia diretamente em "
                f"`{caminho_downloads_usuario()}`."
            )

        # ── Botão para gerar PDFs mensais faltantes ──
        _render_regenerar_mensais_faltantes()

        pasta = Path(PASTA_RELATORIOS)
        if not pasta.exists():
            st.warning("Nenhum relatório gerado ainda.")
            return

        # Coletar todos os PDFs (ignorar arquivos ocultos / JSON)
        pdfs = sorted(pasta.glob("*.pdf"), key=lambda p: p.name)
        if not pdfs:
            st.warning("Nenhum PDF encontrado na pasta `documentacao_anual/`.")
            return

        st.success(f"**{len(pdfs)}** PDF(s) encontrado(s)")

        # Classificar PDFs por tipo
        anuais = []
        mensais = []
        outros = []
        _re_mensal = re.compile(
            r"relatorio_(\d{4})(_local)?_mes_(\d{2})\.pdf"
        )
        _re_anual = re.compile(
            r"relatorio_(\d{4})(_local)?\.pdf$"
        )

        for pdf in pdfs:
            m_mensal = _re_mensal.match(pdf.name)
            m_anual = _re_anual.match(pdf.name)
            if m_mensal:
                mensais.append(pdf)
            elif m_anual:
                anuais.append(pdf)
            else:
                outros.append(pdf)

        # ── Seção: PDFs Anuais ──
        if anuais:
            st.markdown("### 📘 Relatórios Anuais")
            for pdf in anuais:
                m = _re_anual.match(pdf.name)
                ano = m.group(1) if m else "?"
                modo_label = "Automático" if "_local" in pdf.name else "Com IA"
                tamanho = pdf.stat().st_size
                modificado = datetime.fromtimestamp(pdf.stat().st_mtime)
                tamanho_str = (
                    f"{tamanho / (1024*1024):.1f} MB"
                    if tamanho > 1024 * 1024
                    else f"{tamanho / 1024:.0f} KB"
                )

                col_info, col_dl = st.columns([3, 1])
                with col_info:
                    st.markdown(
                        f"**📘 {pdf.name}**  \n"
                        f"Ano: {ano} | Modo: {modo_label} | "
                        f"Tamanho: {tamanho_str} | "
                        f"Modificado: {modificado:%d/%m/%Y %H:%M}"
                    )
                with col_dl:
                    _render_pdf_download_control(
                        str(pdf),
                        pdf.name,
                        "📥 Baixar",
                        f"bib_{pdf.name}",
                    )

        # ── Seção: PDFs Mensais ──
        if mensais:
            st.markdown("### 📅 Relatórios Mensais")
            try:
                from tc_copilot.prompts import obter_nome_mes
            except ImportError:
                obter_nome_mes = None

            # Separar Local e IA
            mensais_local = [
                p for p in mensais if "_local" in p.name
            ]
            mensais_ia = [
                p for p in mensais if "_local" not in p.name
            ]

            def _listar_mensais(lista, titulo):
                if not lista:
                    return
                st.markdown(f"**{titulo}**")
                # Agrupar por ano
                por_ano: dict[str, list] = {}
                for pdf in lista:
                    m = _re_mensal.match(pdf.name)
                    ano_k = m.group(1) if m else "?"
                    por_ano.setdefault(ano_k, []).append(pdf)
                for ano_k in sorted(por_ano, reverse=True):
                    for pdf in por_ano[ano_k]:
                        m = _re_mensal.match(pdf.name)
                        mes_num = int(m.group(3)) if m else 0
                        nome_mes = (
                            obter_nome_mes(mes_num, "pt-BR")
                            if obter_nome_mes and mes_num
                            else f"Mês {mes_num}"
                        )
                        sz = pdf.stat().st_size
                        sz_str = (
                            f"{sz / (1024*1024):.1f} MB"
                            if sz > 1024 * 1024
                            else f"{sz / 1024:.0f} KB"
                        )
                        mod = datetime.fromtimestamp(
                            pdf.stat().st_mtime
                        )
                        col_i, col_d = st.columns([4, 1])
                        with col_i:
                            st.markdown(
                                f"📄 {nome_mes}/{ano_k}"
                                f" — {sz_str}"
                                f" — {mod:%d/%m/%Y %H:%M}"
                            )
                        with col_d:
                            _render_pdf_download_control(
                                str(pdf),
                                pdf.name,
                                "📥",
                                f"bib_{pdf.name}",
                                use_container_width=False,
                            )

            _listar_mensais(mensais_local, "📄 Automático (Local)")
            _listar_mensais(mensais_ia, "🤖 Com IA")

        # ── Outros PDFs ──
        if outros:
            st.markdown("### 📂 Outros")
            for pdf in outros:
                tamanho_kb = pdf.stat().st_size / 1024
                col_info, col_dl = st.columns([3, 1])
                with col_info:
                    st.markdown(f"**{pdf.name}** ({tamanho_kb:.0f} KB)")
                with col_dl:
                    _render_pdf_download_control(
                        str(pdf),
                        pdf.name,
                        "📥 Baixar",
                        f"bib_{pdf.name}",
                    )




    # ═══════════════════════════════════════════════════════════════
    #  DOWNLOADS MENSAIS INDIVIDUAIS
    # ═══════════════════════════════════════════════════════════════

    def _render_downloads_mensais(ano: int, meses_gerados: list[int], modo: str = "local"):
        """Mostra grid de botões de download para PDFs mensais individuais."""
        from tc_copilot.config import caminho_relatorio_mensal
        from tc_copilot.prompts import obter_nome_mes

        if not meses_gerados:
            return

        # Verificar quais PDFs mensais existem
        pdfs_disponiveis = []
        for mes_num in sorted(meses_gerados):
            caminho = str(caminho_relatorio_mensal(ano, mes_num, modo))
            if os.path.exists(caminho):
                pdfs_disponiveis.append((mes_num, caminho))

        if not pdfs_disponiveis:
            return

        st.markdown("##### 📁 PDFs Mensais Individuais")

        # Grid de 4 colunas
        num_cols = min(4, len(pdfs_disponiveis))
        for i in range(0, len(pdfs_disponiveis), num_cols):
            grupo = pdfs_disponiveis[i : i + num_cols]
            cols = st.columns(num_cols)
            for col, (mes_num, caminho) in zip(cols, grupo):
                nome_mes = obter_nome_mes(mes_num, "pt-BR")
                tamanho_kb = os.path.getsize(caminho) / 1024
                with col:
                    _render_pdf_download_control(
                        caminho,
                        os.path.basename(caminho),
                        f"📄 {nome_mes} ({tamanho_kb:.0f} KB)",
                        f"dl_mensal_{modo}_{mes_num}",
                    )


    # ═══════════════════════════════════════════════════════════════
    #  EXIBIÇÃO COMPARTILHADA DO RELATÓRIO (LOCAL / IA)
    # ═══════════════════════════════════════════════════════════════

    def _render_resultado_relatorio(ano: int, idioma: str, modo: str = "ia"):
        """Exibe seções do relatório gerado — reutilizado pelas sub-tabs Local e IA."""
        from tc_copilot.prompts import LABELS

        if modo == "local":
            from tc_copilot.report_generator import carregar_dados_relatorio_local
            dados_relatorio = carregar_dados_relatorio_local(ano)
        else:
            from tc_copilot.report_generator import carregar_dados_relatorio
            dados_relatorio = carregar_dados_relatorio(ano)

        meses_salvos = dados_relatorio.get("meses", {})
        if not meses_salvos:
            return

        st.divider()
        st.subheader("📖 Relatório Completo")

        # Mapeamento tipo_secao → label key (v2 + legado)
        secao_labels = {
            "resumo_executivo": "sec_resumo_executivo",
            "volume_completo": "sec_volume_completo",
            "comparativos": "sec_comparativos",
            "conclusoes": "sec_conclusoes",
            # Legado (compatibilidade com relatórios já gerados)
            "analise_volume": "sec_volume",
            "variacoes_modelo": "sec_variacoes",
            "anomalias": "sec_anomalias",
            "observacoes_finais": "sec_obs_finais",
        }
        # Ordem de prioridade: v2 primeiro, legado se existir
        secoes_v2 = ["resumo_executivo", "volume_completo", "comparativos", "conclusoes"]
        secoes_legado = ["analise_volume", "variacoes_modelo", "comparativos", "anomalias", "observacoes_finais"]
        labels_idioma = LABELS.get(idioma, LABELS["pt-BR"])

        # Obter símbolo de moeda da sessão
        _simbolo_view = st.session_state.get("copilot_simbolo", "R$")

        # Tabs por mês gerado
        meses_ordenados = sorted(meses_salvos.items(), key=lambda x: int(x[0]))
        nomes_tabs = [info.get("mes_nome", f"Mês {num}") for num, info in meses_ordenados]
        tabs_meses = st.tabs(nomes_tabs)

        for tab, (str_mes, info_mes) in zip(tabs_meses, meses_ordenados):
            with tab:
                mes_nome = info_mes.get("mes_nome", f"Mês {str_mes}")
                gerado_em = info_mes.get("gerado_em", "")
                if gerado_em:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(gerado_em)
                        st.caption(f"Gerado em: {dt.strftime('%d/%m/%Y %H:%M')}")
                    except Exception:
                        st.caption(f"Gerado em: {gerado_em}")

                # ── Botão download do PDF individual deste mês ──
                _mes_num_tab = int(str_mes)
                try:
                    from tc_copilot.config import caminho_relatorio_mensal
                    _pdf_mes_path = str(caminho_relatorio_mensal(ano, _mes_num_tab, modo))
                    if os.path.exists(_pdf_mes_path):
                        _sz_kb = os.path.getsize(_pdf_mes_path) / 1024
                        _render_pdf_download_control(
                            _pdf_mes_path,
                            os.path.basename(_pdf_mes_path),
                            f"📥 Baixar PDF de {mes_nome} ({_sz_kb:.0f} KB)",
                            f"dl_tab_{modo}_{_mes_num_tab}",
                            use_container_width=False,
                        )
                except Exception:
                    pass

                secoes = info_mes.get("secoes", {})

                # Detectar se é formato v2 ou legado
                has_v2 = any(k in secoes for k in secoes_v2)
                secoes_ordem = secoes_v2 if has_v2 else secoes_legado

                # ── EXPANDER com seções globais do mês (SEM oficinas) ──
                with st.expander(f"**📄 Relatório de {mes_nome}**", expanded=False):
                    for tipo_secao in secoes_ordem:
                        texto = secoes.get(tipo_secao, "")
                        if not texto:
                            continue
                        label_key = secao_labels.get(tipo_secao, tipo_secao)
                        titulo = labels_idioma.get(label_key, tipo_secao)
                        st.markdown(f"### {titulo}")

                        # ── Gráfico Volume por Veículo na seção Volume ──
                        if tipo_secao == "volume_completo":
                            _inserir_grafico_volume_streamlit(
                                info_mes, mes_nome, ano,
                                simbolo_moeda=_simbolo_view,
                            )
                            st.markdown(texto.replace("$", "\\$"))

                        # ── Comparativos: interleavar gráficos entre sub-tópicos ──
                        elif tipo_secao == "comparativos":
                            _renderizar_comparativos_streamlit(
                                texto, info_mes, mes_nome, ano, _simbolo_view,
                            )

                        else:
                            st.markdown(texto.replace("$", "\\$"))

                        st.markdown("---")

                    _renderizar_tabelas_streamlit(
                        info_mes,
                        titulo_bloco="📊 3.1 Anexos — Tabelas Principais Despesas",
                    )

                # ── EXPANDER separado para análise por oficina ──
                oficina_keys = sorted([k for k in secoes if k.startswith("oficina_")])
                if oficina_keys:
                    with st.expander(
                        f"**🏭 Análise por Oficina — {mes_nome}** "
                        f"({len(oficina_keys)} oficinas)",
                        expanded=False,
                    ):
                        # Gerar dados frescos para sub-tópicos
                        try:
                            from tc_copilot.data_collector import (
                                coletar_dados_mes,
                                calcular_variacoes,
                                formatar_dados_oficina as _fmt_ofc,
                            )
                            _mes_num = int(str_mes)
                            _dados_mes = coletar_dados_mes(ano, _mes_num)
                            _vars_mes = calcular_variacoes(_dados_mes)
                            _dados_frescos_ok = True
                        except Exception:
                            _dados_frescos_ok = False

                        for idx_ofc, ofc_key in enumerate(oficina_keys):
                            ofc_nome = ofc_key.replace("oficina_", "")
                            titulo_template = labels_idioma.get(
                                "sec_oficina",
                                "4.{idx} 🏭 Oficina {oficina}",
                            )
                            titulo_ofc = titulo_template.format(
                                idx=idx_ofc + 1, oficina=ofc_nome,
                            )
                            st.markdown(f"#### {titulo_ofc}")

                            # Tentar renderizar com sub-tópicos estruturados
                            if _dados_frescos_ok:
                                try:
                                    ofc_dict = _fmt_ofc(_dados_mes, _vars_mes, ofc_nome)
                                    # Resumo (Custo FP + deltas)
                                    st.markdown(ofc_dict["resumo"].replace("$", "\\$"))
                                    st.markdown("")

                                    # Sub-tópicos por comparativo com waterfall individual
                                    _sub_topicos = [
                                        ("budget_flex", "📊 Real vs Budget (Efeito Flex Volume)", "budget"),
                                        ("mes_anterior", "📊 Real vs Mês Anterior", "mensal"),
                                        ("ano_anterior", f"📊 Real vs Ano Anterior", "ano_anterior"),
                                    ]
                                    for _tipo, _titulo, _wf_tipo in _sub_topicos:
                                        _conteudo = ofc_dict.get(_tipo, "")
                                        if not _conteudo:
                                            continue
                                        st.markdown(f"**{_titulo}**")
                                        # Inserir waterfall ANTES do texto do sub-tópico
                                        _inserir_waterfall_streamlit(
                                            info_mes, mes_nome, ano,
                                            secao="oficina", ofc_nome=ofc_nome,
                                            tipo_waterfall=_wf_tipo,
                                            simbolo_moeda=_simbolo_view,
                                        )
                                        st.markdown(
                                            _conteudo.replace("$", "\\$"),
                                        )
                                        st.markdown("")

                                    # Análise textual (LLM ou template)
                                    texto_analise = secoes.get(ofc_key, "")
                                    if texto_analise:
                                        label_analise = "💡 Análise IA" if modo == "ia" else "💡 Análise"
                                        st.markdown(f"**{label_analise}**")
                                        st.markdown(texto_analise.replace("$", "\\$"))

                                    _renderizar_tabelas_streamlit(
                                        info_mes,
                                        ofc_nome=ofc_nome,
                                        titulo_bloco="📊 4.1 Anexos — Tabelas Principais Despesas",
                                    )

                                except Exception:
                                    # Fallback: texto salvo pelo report generator
                                    texto = secoes.get(ofc_key, "")
                                    if texto:
                                        st.markdown(texto.replace("$", "\\$"))
                                    _renderizar_tabelas_streamlit(
                                        info_mes,
                                        ofc_nome=ofc_nome,
                                        titulo_bloco="📊 4.1 Anexos — Tabelas Principais Despesas",
                                    )
                            else:
                                # Fallback: texto monolítico salvo
                                texto = secoes.get(ofc_key, "")
                                if texto:
                                    st.markdown(texto.replace("$", "\\$"))
                                _renderizar_tabelas_streamlit(
                                    info_mes,
                                    ofc_nome=ofc_nome,
                                    titulo_bloco="📊 4.1 Anexos — Tabelas Principais Despesas",
                                )

                            st.divider()

        _render_tab()


def _renderizar_tabelas_streamlit(
    info_mes: dict,
    ofc_nome: str | None = None,
    titulo_bloco: str | None = None,
) -> None:
    """Renderiza tabelas já calculadas no JSON do relatório."""
    import pandas as pd

    dados_graf = info_mes.get("dados_graficos", {})
    if ofc_nome is None:
        secao = dados_graf.get("global", {})
    else:
        secao = dados_graf.get("oficinas", {}).get(ofc_nome, {})

    tabelas = secao.get("tabelas", [])
    if not tabelas:
        return

    if titulo_bloco:
        st.markdown(f"**{titulo_bloco}**")

    for idx, tabela in enumerate(tabelas):
        titulo = tabela.get("titulo", f"Tabela {idx + 1}")
        colunas = tabela.get("colunas", [])
        linhas = tabela.get("linhas", [])
        if not colunas or not linhas:
            continue

        df_tab = pd.DataFrame(linhas, columns=colunas)
        for col in df_tab.columns:
            convertido = pd.to_numeric(df_tab[col], errors="coerce")
            if convertido.notna().all():
                df_tab[col] = convertido

        st.markdown(f"*{titulo}*")

        formatters = {
            col: lambda val: f"{val:,.2f}"
            for col in df_tab.select_dtypes(include=["number"]).columns
        }
        if formatters:
            st.dataframe(
                df_tab.style.format(formatters),
                width="stretch",
                hide_index=True,
            )
        else:
            st.dataframe(
                df_tab,
                width="stretch",
                hide_index=True,
            )
        st.markdown("")


# ═══════════════════════════════════════════════════════════════
#  HELPER — COMPARATIVOS: TEXTO + GRÁFICOS INTERLEAVED
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
#  HELPER — GRÁFICO VOLUME POR VEÍCULO
# ═══════════════════════════════════════════════════════════════

def _inserir_grafico_volume_streamlit(
    info_mes: dict, mes_nome: str, ano: int,
    *, simbolo_moeda: str = "R$",
) -> None:
    """Renderiza gráfico de barras Volume Real vs Budget por veículo."""
    dados_graf = info_mes.get("dados_graficos", {})
    graf = dados_graf.get("global", {})
    if not graf:
        return

    vol_real = graf.get("vol_modelos_real", {})
    vol_bud = graf.get("vol_modelos_budget", {})
    if not vol_real and not vol_bud:
        return

    try:
        from tc_copilot.chart_generator import gerar_grafico_volume_por_veiculo
    except ImportError:
        return

    ano_rel = graf.get("ano", ano)
    png = gerar_grafico_volume_por_veiculo(
        vol_modelos_real=vol_real,
        vol_modelos_budget=vol_bud,
        titulo=f"Volume por Veículo — Real vs Budget — {mes_nome}/{ano_rel}",
        transparent=True,
    )
    if png:
        st.image(png, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  HELPER — COMPARATIVOS: TEXTO + GRÁFICOS INTERLEAVED
# ═══════════════════════════════════════════════════════════════

def _renderizar_comparativos_streamlit(
    texto: str,
    info_mes: dict,
    mes_nome: str,
    ano: int,
    simbolo_moeda: str,
) -> None:
    """Renderiza seção Comparativos intercalando gráficos nos sub-tópicos.

    O texto gerado por ``gerar_texto_comparativos`` separa sub-seções com
    o marcador ``<!-- SPLIT -->``. Cada sub-seção possui um header ``### 2.x``.

    - Após o bloco 2.1 (Budget) → chart waterfall Budget
    - Após o bloco 2.2 (Mensal) → chart waterfall Mensal
    """
    import re

    # Separar sub-seções pelo marcador (compatível com textos legados sem marcador)
    if "<!-- SPLIT -->" in texto:
        blocos = [b.strip() for b in texto.split("<!-- SPLIT -->") if b.strip()]
    else:
        # Fallback para textos antigos: separar por ### 2.
        blocos = [b.strip() for b in re.split(r"(?=### 2\.)", texto) if b.strip()]

    for bloco in blocos:
        # Separar título (1ª linha) do corpo analítico
        _linhas = bloco.split("\n", 1)
        titulo_linha = _linhas[0].strip()
        corpo = _linhas[1].strip() if len(_linhas) > 1 else ""

        # 1) Renderizar título do sub-tópico
        st.markdown(titulo_linha.replace("$", "\\$"))

        # 2) Inserir gráfico logo abaixo do título
        _tl = titulo_linha.lstrip("# ")
        if _tl.startswith("2.1"):
            _inserir_waterfall_streamlit(
                info_mes, mes_nome, ano,
                tipo_waterfall="budget",
                simbolo_moeda=simbolo_moeda,
            )
        elif _tl.startswith("2.2"):
            _inserir_waterfall_streamlit(
                info_mes, mes_nome, ano,
                tipo_waterfall="mensal",
                simbolo_moeda=simbolo_moeda,
            )
        elif _tl.startswith("2.3"):
            _inserir_waterfall_streamlit(
                info_mes, mes_nome, ano,
                tipo_waterfall="ano_anterior",
                simbolo_moeda=simbolo_moeda,
            )

        # 3) Renderizar corpo do texto
        if corpo:
            st.markdown(corpo.replace("$", "\\$"))


# ═══════════════════════════════════════════════════════════════
#  HELPER — GRÁFICOS WATERFALL NA ABA RELATÓRIO
# ═══════════════════════════════════════════════════════════════

def _inserir_waterfall_streamlit(
    info_mes: dict, mes_nome: str, ano: int,
    *, secao: str = "global",
    ofc_nome: str | None = None,
    tipo_waterfall: str = "ambos",
    simbolo_moeda: str = "R$",
) -> None:
    """Renderiza gráficos waterfall (Account, CPU) no Streamlit com fundo transparente.

    tipo_waterfall: "budget", "mensal", "ano_anterior" ou "ambos".
    """
    dados_graf = info_mes.get("dados_graficos", {})

    try:
        from tc_copilot.chart_generator import gerar_waterfall_from_arrays
    except ImportError:
        return

    cpu_label = f"{simbolo_moeda}/veíc"

    if secao == "global":
        graf = dados_graf.get("global", {})
        if not graf:
            return
        ano_rel = graf.get("ano", ano)

        # Helper para inserir par Type 05 + Type 06
        def _inserir_type05_type06(prefixo: str, label_comp: str):
            for sfx, dim_label, h in [
                ("type05", "Type 05", 3.5),
                ("type06", "Type 06", 5),
            ]:
                _lbls = graf.get(f"{prefixo}_{sfx}_labels", [])
                _vals = graf.get(f"{prefixo}_{sfx}_values", [])
                if _lbls and len(_lbls) >= 3:
                    _png = gerar_waterfall_from_arrays(
                        {"labels": _lbls, "values": _vals},
                        titulo=f"{dim_label} — {label_comp} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                        transparent=True,
                        y_label=cpu_label,
                        height=h,
                    )
                    if _png:
                        st.image(_png, use_container_width=True)

        # Waterfall Budget
        if tipo_waterfall in ("budget", "ambos"):
            # Type 05 + Type 06
            _inserir_type05_type06("wf_budget", "Waterfall Budget")
            # Account
            wf_bud_labels = graf.get("wf_budget_labels", [])
            wf_bud_values = graf.get("wf_budget_values", [])
            if wf_bud_labels and len(wf_bud_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_bud_labels, "values": wf_bud_values},
                    titulo=f"Account — Waterfall Budget — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                    transparent=True,
                    y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        # Waterfall Mensal
        if tipo_waterfall in ("mensal", "ambos"):
            _inserir_type05_type06("wf_mensal", "Waterfall Mensal")
            wf_men_labels = graf.get("wf_mensal_labels", [])
            wf_men_values = graf.get("wf_mensal_values", [])
            if wf_men_labels and len(wf_men_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_men_labels, "values": wf_men_values},
                    titulo=f"Account — Waterfall Mensal — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                    transparent=True,
                    y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        # Waterfall Ano Anterior (YoY)
        if tipo_waterfall in ("ano_anterior", "ambos"):
            _inserir_type05_type06("wf_ano_ant", "Waterfall Ano Anterior")
            wf_aa_labels = graf.get("wf_ano_ant_labels", [])
            wf_aa_values = graf.get("wf_ano_ant_values", [])
            ano_ant_rel = graf.get("ano_anterior", ano_rel - 1)
            if wf_aa_labels and len(wf_aa_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_aa_labels, "values": wf_aa_values},
                    titulo=f"Account — Waterfall Ano Anterior — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}",
                    transparent=True,
                    y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

    elif secao == "oficina" and ofc_nome:
        graf_oficinas = dados_graf.get("oficinas", {})
        graf = graf_oficinas.get(ofc_nome, {})
        if not graf:
            return
        ano_rel = graf.get("ano", ano)

        # Helper para inserir par Type 05 + Type 06 (oficina)
        def _inserir_type05_type06_ofc(prefixo: str, label_comp: str):
            for sfx, dim_label, h in [
                ("type05", "Type 05", 3.5),
                ("type06", "Type 06", 5),
            ]:
                _lbls = graf.get(f"{prefixo}_{sfx}_labels", [])
                _vals = graf.get(f"{prefixo}_{sfx}_values", [])
                if _lbls and len(_lbls) >= 3:
                    _png = gerar_waterfall_from_arrays(
                        {"labels": _lbls, "values": _vals},
                        titulo=f"{dim_label} — {label_comp} — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                        transparent=True,
                        y_label=cpu_label,
                        height=h,
                    )
                    if _png:
                        st.image(_png, use_container_width=True)

        # Selecionar tipo de waterfall
        if tipo_waterfall in ("budget", "ambos"):
            _inserir_type05_type06_ofc("wf_budget", "Waterfall Budget")
            wf_labels = graf.get("wf_budget_labels", [])
            wf_values = graf.get("wf_budget_values", [])
            titulo_wf = f"Account — Waterfall Budget — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}"
            if wf_labels and len(wf_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_labels, "values": wf_values},
                    titulo=titulo_wf, transparent=True, y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        if tipo_waterfall in ("mensal", "ambos"):
            _inserir_type05_type06_ofc("wf_mensal", "Waterfall Mensal")
            wf_labels = graf.get("wf_mensal_labels", [])
            wf_values = graf.get("wf_mensal_values", [])
            titulo_wf = f"Account — Waterfall Mensal — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}"
            if wf_labels and len(wf_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_labels, "values": wf_values},
                    titulo=titulo_wf, transparent=True, y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        if tipo_waterfall in ("ano_anterior", "ambos"):
            _inserir_type05_type06_ofc("wf_ano_ant", "Waterfall Ano Anterior")
            wf_labels = graf.get("wf_ano_ant_labels", [])
            wf_values = graf.get("wf_ano_ant_values", [])
            ano_ant_rel = graf.get("ano_anterior", ano_rel - 1)
            titulo_wf = f"Waterfall Ano Anterior — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}"
            if wf_labels and len(wf_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_labels, "values": wf_values},
                    titulo=titulo_wf, transparent=True, y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)


# ═══════════════════════════════════════════════════════════════
#  ABA 3 — CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════

def _render_configuracao():
    st.subheader("Configuração do TC Copilot")

    # ── Toggle habilitar/desabilitar Copilot ──
    habilitado = carregar_copilot_habilitado()
    novo_estado = st.toggle("TC Copilot ativo", value=habilitado, key="toggle_copilot")
    if novo_estado != habilitado:
        salvar_copilot_habilitado(novo_estado)
        st.rerun()

    if not habilitado:
        st.info("Ative o toggle acima para usar o Chatbot e Relatório.")
        return

    # ── Provider e credenciais ──
    st.markdown("#### Provedor LLM")
    provider_atual = carregar_provider()
    provider_opcoes = ["openai", "databricks_claude"]
    provider = st.selectbox(
        "Provedor",
        provider_opcoes,
        index=provider_opcoes.index(provider_atual) if provider_atual in provider_opcoes else 0,
        format_func=lambda x: {
            "openai": "OpenAI",
            "databricks_claude": "Databricks Claude",
        }.get(x, x),
        help="Selecione qual backend LLM o TC Copilot deve usar.",
    )
    if provider != provider_atual:
        salvar_provider(provider)
        st.rerun()

    chave_atual = carregar_api_key()
    databricks_cfg = carregar_databricks_cfg()

    if provider == "databricks_claude":
        status_provider = "✅ Configurado" if all(databricks_cfg.values()) else "❌ Não configurado"
        st.markdown("#### Credenciais do Databricks")
        st.info(f"Status: {status_provider}")

        with st.form("form_databricks_cfg"):
            nova_url = st.text_input(
                "URL do workspace Databricks",
                value=databricks_cfg["url"],
                placeholder="https://adb-....azuredatabricks.net",
                help="Se vazio, o backend usa DATABRICKS_HOST quando existir.",
            )
            novo_endpoint = st.text_input(
                "Serving endpoint",
                value=databricks_cfg["endpoint"],
                placeholder="databricks-claude-opus-4-6",
                help="Nome do endpoint de Model Serving que receberá as requisições.",
            )
            _token_ok = bool(databricks_cfg.get("token"))
            if _token_ok:
                st.caption("✅ Token Databricks configurado.")
            else:
                st.warning("⚠️ Token Databricks não configurado. Preencha o campo abaixo.")
            atualizar_token = st.checkbox(
                "Atualizar token Databricks",
                value=not _token_ok,
                key="copilot_atualizar_token_databricks",
            )
            novo_token = ""
            if atualizar_token:
                st.markdown(
                    """
                    <style>
                    div[data-testid="stTextArea"] textarea {
                        -webkit-text-security: disc;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                novo_token = st.text_area(
                    "Novo token Databricks",
                    value="",
                    placeholder="dapi...",
                    help="PAT ou token aceito pelo workspace Databricks. O valor digitado substitui o token atual.",
                    height=68,
                    key="copilot_novo_token_databricks",
                )
            salvar_cfg = st.form_submit_button("💾 Salvar Databricks", use_container_width=True)
        if salvar_cfg:
            token_para_salvar = databricks_cfg["token"]
            if atualizar_token:
                token_para_salvar = novo_token.strip()
            salvar_databricks_cfg(nova_url, novo_endpoint, token_para_salvar)
            st.success("Configuração do Databricks salva com sucesso!")
            st.rerun()
    else:
        status_chave = "✅ Configurada" if chave_atual else "❌ Não configurada"
        st.markdown("#### Chave da OpenAI")
        st.info(f"Status: {status_chave}")

        with st.form("form_api_key"):
            nova_chave = st.text_input(
                "Chave da API OpenAI",
                value=chave_atual or "",
                type="password",
                placeholder="sk-...",
                help="Insira sua chave da OpenAI. Ela será salva no arquivo .env local.",
            )
            salvar = st.form_submit_button("💾 Salvar", use_container_width=True)
        if salvar and nova_chave.strip():
            salvar_api_key(nova_chave.strip())
            st.success("Chave salva com sucesso!")
            st.rerun()

    # ── Modelo LLM ──
    st.markdown("#### Modelo LLM")
    modelo_atual = carregar_modelo()
    modelos_disponiveis = MODELOS_DATABRICKS if provider == "databricks_claude" else MODELOS_LLM
    modelo_idx = modelos_disponiveis.index(modelo_atual) if modelo_atual in modelos_disponiveis else 0
    modelo = st.selectbox(
        "Modelo",
        modelos_disponiveis,
        index=modelo_idx,
        help=(
            "Selecione o modelo OpenAI desejado."
            if provider == "openai"
            else "Selecione o serving endpoint/modelo padrão do Databricks Claude."
        ),
    )
    if modelo != modelo_atual:
        salvar_modelo(modelo)
    st.session_state["copilot_modelo"] = modelo

    # ── Idioma ──
    st.markdown("#### Idioma do Relatório")
    idioma_atual = carregar_idioma()
    idiomas_list = list(IDIOMAS.keys())
    idioma_idx = idiomas_list.index(idioma_atual) if idioma_atual in idiomas_list else 0
    idioma = st.selectbox(
        "Idioma",
        idiomas_list,
        index=idioma_idx,
        format_func=lambda x: IDIOMAS[x],
        help="Define o idioma do relatório PDF e das análises.",
    )
    if idioma != idioma_atual:
        salvar_idioma(idioma)
    st.session_state["copilot_idioma"] = idioma

    # ── Moeda do Relatório ──
    st.markdown("#### 💱 Moeda do Relatório")
    try:
        from tc_core.finance.currency_db import carregar_taxas_banco, salvar_taxas_banco, inicializar_banco_taxas
        from tc_core.finance.currency import obter_simbolo_moeda
        inicializar_banco_taxas()
        taxas_entrada = carregar_taxas_banco()
    except ImportError:
        taxas_entrada = {"USD": 5.0, "EUR": 6.4855}

    moedas_opcoes = ["BRL", "USD", "EUR"]
    moeda_atual = st.session_state.get("copilot_moeda", "EUR")
    moeda_idx = moedas_opcoes.index(moeda_atual) if moeda_atual in moedas_opcoes else 0

    moeda = st.radio(
        "Moeda",
        moedas_opcoes,
        index=moeda_idx,
        horizontal=True,
        key="copilot_moeda_radio",
        format_func=lambda x: {"BRL": "🇧🇷 R$ (BRL)", "USD": "🇺🇸 $ (USD)", "EUR": "🇪🇺 € (EUR)"}.get(x, x),
    )
    st.session_state["copilot_moeda"] = moeda

    if moeda != "BRL":
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            taxas_entrada["USD"] = st.number_input(
                "🇺🇸 1 USD = R$",
                value=taxas_entrada.get("USD", 5.0),
                min_value=0.001, step=0.001, format="%.3f",
                key="copilot_taxa_usd",
            )
        with col_t2:
            taxas_entrada["EUR"] = st.number_input(
                "🇪🇺 1 EUR = R$",
                value=taxas_entrada.get("EUR", 6.4855),
                min_value=0.001, step=0.001, format="%.3f",
                key="copilot_taxa_eur",
            )
        try:
            salvar_taxas_banco(taxas_entrada)
        except Exception:
            pass

    # Calcular taxas inversas (1 BRL → X moeda) para conversão
    taxa_usd = taxas_entrada.get("USD", 5.0)
    taxa_eur = taxas_entrada.get("EUR", 6.4855)
    taxas_inversas = {
        "BRL": 1.0,
        "USD": 1.0 / taxa_usd if taxa_usd > 0 else 0.20,
        "EUR": 1.0 / taxa_eur if taxa_eur > 0 else 1.0 / 6.4855,
    }
    st.session_state["copilot_taxas"] = taxas_inversas

    simbolo = {"BRL": "R$", "USD": "$", "EUR": "€"}.get(moeda, "R$")
    st.session_state["copilot_simbolo"] = simbolo

    # ── Resumo ──
    st.divider()
    st.markdown("**Configuração ativa:**")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Provider", "OpenAI" if provider == "openai" else "Databricks")
    col2.metric(
        "Credenciais",
        "✅" if (bool(chave_atual) if provider == "openai" else all(databricks_cfg.values())) else "❌",
    )
    col3.metric("Idioma", IDIOMAS.get(idioma, idioma))
    col4.metric("Modelo", modelo)

    st.caption(f"Moeda ativa: {moeda}")

    # ── Bibliotecas e Roadmap ──
    st.divider()
    st.markdown("#### 📦 Bibliotecas em uso")
    libs = {
        "Python": "Linguagem principal",
        "Pandas": "Manipulação de dados",
        "Parquet": "Armazenamento eficiente de dados",
        "ReportLab": "Geração de PDFs",
        "OpenAI / Databricks Model Serving": "Geração de textos via LLM",
        "Streamlit": "Interface web interativa",
        "NumPy": "Cálculos numéricos",
    }
    for lib, desc in libs.items():
        st.markdown(f"- **{lib}**: {desc}")

    st.markdown("#### 🔮 Roadmap de melhorias")
    roadmap = {
        "LangChain": "Integração avançada com LLMs e workflows — memória de conversa nativa",
        "WeasyPrint": "PDFs a partir de HTML/CSS — layouts mais flexíveis",
        "Plotly/Altair": "Gráficos interativos no relatório",
        "FastAPI": "API desacoplada para integrações externas",
    }
    for lib, desc in roadmap.items():
        st.markdown(f"- **{lib}**: {desc}")
