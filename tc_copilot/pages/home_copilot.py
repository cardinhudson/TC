"""
TC Copilot — Página Streamlit principal.

Três abas:
  1. 💬 Chatbot — consulta inteligente ao vivo (dados parquet)
  2. 📄 Relatório — gerar relatório PDF e exibir na tela
     2a. Relatório Automático (sem API)
     2b. Relatório com IA (OpenAI)
     2c. Biblioteca de PDFs — visualizar/baixar todos os PDFs gerados
  3. ⚙️ Configuração — API key, modelo, idioma
"""

from __future__ import annotations

import os
import streamlit as st

from tc_copilot.config import (
    IDIOMAS,
    MODELOS_LLM,
    PASTA_RELATORIOS,
    carregar_api_key,
    carregar_idioma,
    carregar_modelo,
    caminho_relatorio,
    salvar_api_key,
    carregar_copilot_habilitado,
    salvar_copilot_habilitado,
)

import re as _re

# ═══════════════════════════════════════════════════════════════
#  INFERÊNCIA DE MÊS A PARTIR DA PERGUNTA
# ═══════════════════════════════════════════════════════════════

_MESES_MAP: dict[str, int] = {
    # pt-BR
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
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
        _render_chatbot()

    with tab_relatorio:
        _render_gerar_relatorio()

    with tab_config:
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
    )
    from tc_copilot.llm_integration import responder_consulta_live
    from tc_copilot.prompts import obter_nome_mes

    st.subheader("Consulta Inteligente")
    st.caption("Pergunte diretamente sobre os dados de custos — acesso ao vivo nos parquets")

    api_key = carregar_api_key()
    if not api_key:
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
        ano = st.selectbox("Ano", anos, key="chat_ano")
    with col_mes:
        meses_disp = descobrir_meses_disponiveis(ano)
        if not meses_disp:
            st.warning(f"Nenhum mês com dados para {ano}.")
            return
        opcoes_meses = {obter_nome_mes(m, "pt-BR"): m for m in meses_disp}
        # Primeira opção vazia para inferir da pergunta
        opcoes_display = ["(inferir da pergunta)"] + list(opcoes_meses.keys())
        mes_label = st.selectbox(
            "Mês",
            opcoes_display,
            index=len(opcoes_display) - 1,  # Default: último mês
            key="chat_mes",
        )
        if mes_label == "(inferir da pergunta)":
            mes_numero = None
        else:
            mes_numero = opcoes_meses[mes_label]

    # ── Histórico de chat ──
    _chat_mes = mes_numero if mes_numero is not None else "auto"
    chat_key = f"copilot_chat_{ano}_{_chat_mes}"
    if chat_key not in st.session_state:
        st.session_state[chat_key] = []

    historico = st.session_state[chat_key]

    # Mostrar histórico
    for msg in historico:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"].replace("$", "\\$"))

    # ── Input do usuário ──
    _input_label = f"Pergunte sobre {mes_label}/{ano}..." if mes_numero else f"Pergunte sobre {ano} (mês será inferido)..."
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
                # Inferir mês da pergunta se não selecionado
                _mes_efetivo = mes_numero
                _mes_inferido = False
                if _mes_efetivo is None:
                    _mes_efetivo = _inferir_mes_da_pergunta(pergunta, meses_disp)
                    _mes_inferido = True

                # Configurar moeda antes de formatar contexto
                moeda = st.session_state.get("copilot_moeda", "EUR")
                simbolo = st.session_state.get("copilot_simbolo", "€")
                taxas = st.session_state.get("copilot_taxas", {"BRL": 1.0, "USD": 0.20, "EUR": 0.18})
                taxa_conversao = taxas.get(moeda, 1.0)
                configurar_moeda_formatacao(moeda, simbolo)

                contexto = formatar_contexto_parquet(ano, _mes_efetivo, taxa_conversao=taxa_conversao)
                if _mes_inferido:
                    _nome_mes_inf = obter_nome_mes(_mes_efetivo, "pt-BR")
                    contexto = (
                        f"\u26a0\ufe0f Mês inferido da pergunta: {_nome_mes_inf}/{ano}\n\n"
                        + contexto
                    )

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
            f"Quais foram os maiores desvios de Labor em {mes_label} de {ano}?",
            f"Qual oficina teve maior impacto no custo em {mes_label}?",
            f"Compare Labor, Burden e D&A vs Flex Budget em {mes_label}",
            f"Quais modelos tiveram maior variação de volume vs mês anterior?",
            "Houve alguma anomalia significativa nos custos?",
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
            with open(pdf_existente, "rb") as f:
                st.download_button(
                    label="📥 Baixar PDF Anual",
                    data=f.read(),
                    file_name=f"relatorio_tc_{ano}_local.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="dl_pdf_local",
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

    api_key = carregar_api_key()
    if not api_key:
        st.warning(
            "⚠️ Chave da OpenAI não configurada. "
            "O relatório será gerado sem análise inteligente (dados brutos apenas). "
            "Configure na aba **⚙️ Configuração**."
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
            with open(pdf_existente, "rb") as f:
                st.download_button(
                    label="📥 Baixar PDF Anual",
                    data=f.read(),
                    file_name=f"relatorio_tc_{ano}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
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
        progress = st.progress(0, text="Gerando...")
        ok = 0
        for i, (ano, mes, modo) in enumerate(faltantes):
            progress.progress(
                (i + 1) / len(faltantes),
                text=f"Gerando {modo} {mes:02d}/{ano}...",
            )
            try:
                r = gerar_pdf_mensal(ano, mes, modo=modo)
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
                with open(pdf, "rb") as f:
                    st.download_button(
                        label="📥 Baixar",
                        data=f.read(),
                        file_name=pdf.name,
                        mime="application/pdf",
                        use_container_width=True,
                        key=f"bib_{pdf.name}",
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
                        with open(pdf, "rb") as f:
                            st.download_button(
                                "📥",
                                data=f.read(),
                                file_name=pdf.name,
                                mime="application/pdf",
                                key=f"bib_{pdf.name}",
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
                with open(pdf, "rb") as f:
                    st.download_button(
                        label="📥 Baixar",
                        data=f.read(),
                        file_name=pdf.name,
                        mime="application/pdf",
                        use_container_width=True,
                        key=f"bib_{pdf.name}",
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
                with open(caminho, "rb") as f:
                    st.download_button(
                        label=f"📄 {nome_mes} ({tamanho_kb:.0f} KB)",
                        data=f.read(),
                        file_name=os.path.basename(caminho),
                        mime="application/pdf",
                        use_container_width=True,
                        key=f"dl_mensal_{modo}_{mes_num}",
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
                    with open(_pdf_mes_path, "rb") as _f_pdf:
                        st.download_button(
                            label=f"📥 Baixar PDF de {mes_nome} ({_sz_kb:.0f} KB)",
                            data=_f_pdf.read(),
                            file_name=os.path.basename(_pdf_mes_path),
                            mime="application/pdf",
                            key=f"dl_tab_{modo}_{_mes_num_tab}",
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

                            except Exception:
                                # Fallback: texto salvo pelo report generator
                                texto = secoes.get(ofc_key, "")
                                if texto:
                                    st.markdown(texto.replace("$", "\\$"))
                        else:
                            # Fallback: texto monolítico salvo
                            texto = secoes.get(ofc_key, "")
                            if texto:
                                st.markdown(texto.replace("$", "\\$"))

                        st.divider()


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

        # Waterfall Budget
        if tipo_waterfall in ("budget", "ambos"):
            wf_bud_labels = graf.get("wf_budget_labels", [])
            wf_bud_values = graf.get("wf_budget_values", [])
            if wf_bud_labels and len(wf_bud_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_bud_labels, "values": wf_bud_values},
                    titulo=f"Waterfall Budget — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                    transparent=True,
                    y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        # Waterfall Mensal
        if tipo_waterfall in ("mensal", "ambos"):
            wf_men_labels = graf.get("wf_mensal_labels", [])
            wf_men_values = graf.get("wf_mensal_values", [])
            if wf_men_labels and len(wf_men_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_men_labels, "values": wf_men_values},
                    titulo=f"Waterfall Mensal — CPU ({cpu_label}) — {mes_nome}/{ano_rel}",
                    transparent=True,
                    y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        # Waterfall Ano Anterior (YoY)
        if tipo_waterfall in ("ano_anterior", "ambos"):
            wf_aa_labels = graf.get("wf_ano_ant_labels", [])
            wf_aa_values = graf.get("wf_ano_ant_values", [])
            ano_ant_rel = graf.get("ano_anterior", ano_rel - 1)
            if wf_aa_labels and len(wf_aa_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_aa_labels, "values": wf_aa_values},
                    titulo=f"Waterfall Ano Anterior — CPU ({cpu_label}) — {mes_nome}/{ano_ant_rel} vs {ano_rel}",
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

        # Selecionar tipo de waterfall
        if tipo_waterfall in ("budget", "ambos"):
            wf_labels = graf.get("wf_budget_labels", [])
            wf_values = graf.get("wf_budget_values", [])
            titulo_wf = f"Waterfall Budget — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}"
            if wf_labels and len(wf_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_labels, "values": wf_values},
                    titulo=titulo_wf, transparent=True, y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        if tipo_waterfall in ("mensal", "ambos"):
            wf_labels = graf.get("wf_mensal_labels", [])
            wf_values = graf.get("wf_mensal_values", [])
            titulo_wf = f"Waterfall Mensal — {ofc_nome} — CPU ({cpu_label}) — {mes_nome}/{ano_rel}"
            if wf_labels and len(wf_labels) >= 3:
                png = gerar_waterfall_from_arrays(
                    {"labels": wf_labels, "values": wf_values},
                    titulo=titulo_wf, transparent=True, y_label=cpu_label,
                )
                if png:
                    st.image(png, use_container_width=True)

        if tipo_waterfall in ("ano_anterior", "ambos"):
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

    # ── API Key ──
    st.markdown("#### Chave da OpenAI")
    chave_atual = carregar_api_key()
    status_chave = "✅ Configurada" if chave_atual else "❌ Não configurada"
    st.info(f"Status: {status_chave}")

    with st.form("form_api_key"):
        nova_chave = st.text_input(
            "Chave da API OpenAI",
            type="password",
            placeholder="sk-...",
            help="Insira sua chave da OpenAI. Ela será salva no arquivo .env local.",
        )
        col1, col2 = st.columns([1, 3])
        with col1:
            salvar = st.form_submit_button("💾 Salvar", use_container_width=True)
        if salvar and nova_chave.strip():
            salvar_api_key(nova_chave.strip())
            st.success("Chave salva com sucesso!")
            st.rerun()

    # ── Modelo LLM ──
    st.markdown("#### Modelo LLM")
    modelo_atual = carregar_modelo()
    modelo_idx = MODELOS_LLM.index(modelo_atual) if modelo_atual in MODELOS_LLM else 0
    modelo = st.selectbox(
        "Modelo",
        MODELOS_LLM,
        index=modelo_idx,
        help="gpt-4o-mini é mais rápido e econômico. gpt-4o oferece melhor qualidade.",
    )
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
    st.session_state["copilot_idioma"] = idioma

    # ── Moeda do Relatório ──
    st.markdown("#### 💱 Moeda do Relatório")
    try:
        from tc_core.finance.currency_db import carregar_taxas_banco, salvar_taxas_banco, inicializar_banco_taxas
        from tc_core.finance.currency import obter_simbolo_moeda
        inicializar_banco_taxas()
        taxas_entrada = carregar_taxas_banco()
    except ImportError:
        taxas_entrada = {"USD": 5.0, "EUR": 5.5}

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
                min_value=0.01, step=0.01, format="%.2f",
                key="copilot_taxa_usd",
            )
        with col_t2:
            taxas_entrada["EUR"] = st.number_input(
                "🇪🇺 1 EUR = R$",
                value=taxas_entrada.get("EUR", 5.5),
                min_value=0.01, step=0.01, format="%.2f",
                key="copilot_taxa_eur",
            )
        try:
            salvar_taxas_banco(taxas_entrada)
        except Exception:
            pass

    # Calcular taxas inversas (1 BRL → X moeda) para conversão
    taxa_usd = taxas_entrada.get("USD", 5.0)
    taxa_eur = taxas_entrada.get("EUR", 5.5)
    taxas_inversas = {
        "BRL": 1.0,
        "USD": 1.0 / taxa_usd if taxa_usd > 0 else 0.20,
        "EUR": 1.0 / taxa_eur if taxa_eur > 0 else 0.18,
    }
    st.session_state["copilot_taxas"] = taxas_inversas

    simbolo = {"BRL": "R$", "USD": "$", "EUR": "€"}.get(moeda, "R$")
    st.session_state["copilot_simbolo"] = simbolo

    # ── Resumo ──
    st.divider()
    st.markdown("**Configuração ativa:**")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("API Key", "✅" if chave_atual else "❌")
    col2.metric("Modelo", modelo)
    col3.metric("Idioma", IDIOMAS.get(idioma, idioma))
    col4.metric("Moeda", moeda)

    # ── Bibliotecas e Roadmap ──
    st.divider()
    st.markdown("#### 📦 Bibliotecas em uso")
    libs = {
        "Python": "Linguagem principal",
        "Pandas": "Manipulação de dados",
        "Parquet": "Armazenamento eficiente de dados",
        "ReportLab": "Geração de PDFs",
        "OpenAI API": "Geração de textos via LLM",
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
