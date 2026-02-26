"""
TC Copilot — Página Streamlit principal.

Três abas:
  1. 💬 Chatbot — consulta inteligente ao vivo (dados parquet)
  2. 📄 Relatório — gerar relatório PDF e exibir na tela
  3. ⚙️ Configuração — API key, modelo, idioma
"""

from __future__ import annotations

import os
import streamlit as st

from tc_copilot.config import (
    IDIOMAS,
    MODELOS_LLM,
    carregar_api_key,
    carregar_idioma,
    carregar_modelo,
    caminho_relatorio,
    salvar_api_key,
    carregar_copilot_habilitado,
    salvar_copilot_habilitado,
)


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
        opcoes = {obter_nome_mes(m, "pt-BR"): m for m in meses_disp}
        # Selecionar o último mês disponível por padrão
        mes_label = st.selectbox(
            "Mês",
            list(opcoes.keys()),
            index=len(opcoes) - 1,
            key="chat_mes",
        )
        mes_numero = opcoes[mes_label]

    # ── Histórico de chat ──
    chat_key = f"copilot_chat_{ano}_{mes_numero}"
    if chat_key not in st.session_state:
        st.session_state[chat_key] = []

    historico = st.session_state[chat_key]

    # Mostrar histórico
    for msg in historico:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"].replace("$", "\\$"))

    # ── Input do usuário ──
    pergunta = st.chat_input(
        f"Pergunte sobre {mes_label}/{ano}...",
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
                contexto = formatar_contexto_parquet(ano, mes_numero)

                modelo = st.session_state.get("copilot_modelo", "gpt-4o-mini")
                idioma = st.session_state.get("copilot_idioma", "pt-BR")

                resposta = responder_consulta_live(
                    pergunta=pergunta,
                    contexto_dados=contexto,
                    historico_chat=historico[:-1],  # Sem a pergunta atual
                    idioma=idioma,
                    api_key=api_key,
                    model=modelo,
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
    """Renderiza a aba de relatório com sub-tabs: Automático (sem API) e Com IA."""
    st.subheader("Gerar Relatório Anual")

    sub_auto, sub_ia = st.tabs([
        "📄 Relatório Automático",
        "🤖 Relatório com IA",
    ])

    with sub_auto:
        _render_relatorio_local()

    with sub_ia:
        _render_relatorio_ia()


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

    # ── Seletor de mês ──
    opcoes_meses = {
        f"{obter_nome_mes(m, 'pt-BR')} {'✅' if m in meses_gerados else ''}": m
        for m in meses_disp
    }
    mes_selecionado_label = st.selectbox(
        "Mês a gerar/regenerar",
        list(opcoes_meses.keys()),
        key="rel_local_mes",
    )
    mes_selecionado = opcoes_meses[mes_selecionado_label]

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
        meses_para_gerar = meses_disp if gerar_todos else [mes_selecionado]
        # Obter configuração de moeda da sessão
        _moeda = st.session_state.get("copilot_moeda", "BRL")
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
                    label="📥 Baixar PDF",
                    data=f.read(),
                    file_name=f"relatorio_tc_{ano}_local.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="dl_pdf_local",
                )

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

    # ── Selecionar mês para gerar ──
    opcoes_meses = {
        f"{obter_nome_mes(m, 'pt-BR')} {'✅' if m in meses_gerados else ''}": m
        for m in meses_disp
    }
    mes_selecionado_label = st.selectbox(
        "Mês a gerar/regenerar",
        list(opcoes_meses.keys()),
        key="rel_mes",
    )
    mes_selecionado = opcoes_meses[mes_selecionado_label]

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
        meses_para_gerar = meses_disp if gerar_todos else [mes_selecionado]

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
                pdf_path = gerar_relatorio_mes(
                    ano=ano,
                    mes_numero=mes_num,
                    api_key=api_key,
                    modelo=modelo,
                    idioma=idioma,
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
                    label="📥 Baixar PDF",
                    data=f.read(),
                    file_name=f"relatorio_tc_{ano}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )

    # ── Exibir relatório ──
    _render_resultado_relatorio(ano, idioma, modo="ia")


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

                    # ── Gráfico waterfall Budget na seção Volume ──
                    if tipo_secao == "volume_completo":
                        _inserir_waterfall_streamlit(
                            info_mes, mes_nome, ano,
                            tipo_waterfall="budget",
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

                    for ofc_key in oficina_keys:
                        ofc_nome = ofc_key.replace("oficina_", "")
                        titulo_template = labels_idioma.get(
                            "sec_oficina", "🏭 Oficina {oficina}"
                        )
                        titulo_ofc = titulo_template.format(oficina=ofc_nome)
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
        # Inserir gráfico ANTES do texto do sub-tópico
        if bloco.lstrip().startswith("### 2.1"):
            _inserir_waterfall_streamlit(
                info_mes, mes_nome, ano,
                tipo_waterfall="budget",
                simbolo_moeda=simbolo_moeda,
            )
        elif bloco.lstrip().startswith("### 2.2"):
            _inserir_waterfall_streamlit(
                info_mes, mes_nome, ano,
                tipo_waterfall="mensal",
                simbolo_moeda=simbolo_moeda,
            )
        elif bloco.lstrip().startswith("### 2.3"):
            _inserir_waterfall_streamlit(
                info_mes, mes_nome, ano,
                tipo_waterfall="ano_anterior",
                simbolo_moeda=simbolo_moeda,
            )

        # Renderizar texto do bloco
        st.markdown(bloco.replace("$", "\\$"))


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
    moeda_atual = st.session_state.get("copilot_moeda", "BRL")
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
