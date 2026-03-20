"""Página Streamlit — Monitoramento de Alertas.

Exibe **um card consolidado** com ranking hierárquico de perdas:
Type 05 → Type 06 → Account → Oficinas → Texto breve.
"""

from __future__ import annotations

from datetime import date

import streamlit as st

from alertas.alert_engine import (
    append_to_alert_log,
    accounts_disponiveis,
    build_alert_log_entry,
    calcular_ranking_consolidado,
    fmt_delta_k,
    fmt_k,
    fmt_linha_oficina,
    gerar_tabela_validacao,
    load_alert_log,
    load_alert_rules,
    load_all_data,
    MODOS_COMPARACAO,
    oficinas_disponiveis,
    periodos_disponiveis,
    type05_disponiveis,
    type06_disponiveis,
)
from alertas.utils_dates import (
    mes_atual_nome,
    proporcao_mes,
)


# =========================================================================
#  CSS
# =========================================================================

_CSS = """
<style>
.block-container {
    padding-top: 4rem !important;
    padding-bottom: 0.5rem !important;
}
hr {
    display: none !important;
    margin: 0 !important;
}
[data-testid="stVerticalBlock"] > div {
    margin-bottom: 0.35rem;
}
/* --- Card consolidado --- */
.alc-card {
    border: 1px solid #ddd;
    border-radius: 12px;
    padding: 16px 20px;
    margin-bottom: 10px;
    background: #fafafa;
}
@media (prefers-color-scheme: dark) {
    .alc-card { background: #1e1e1e; border-color: #444; }
    .alc-meta, .alc-legenda { color: #aaa !important; }
    .alc-acc { color: #bbb !important; }
    .alc-ofi-name { color: #ccc !important; }
    .alc-txt { color: #999 !important; }
}

.alc-header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 10px; padding-bottom: 8px;
    border-bottom: 2px solid #e0e0e0;
}
.alc-title { font-size: 1.25em; font-weight: 800; }
.alc-subtitle { font-size: 0.88em; opacity: 0.75; margin-top: 2px; }

/* Badges */
.alc-badge {
    display: inline-block; padding: 3px 12px; border-radius: 12px;
    font-size: 0.78em; font-weight: 700; color: #fff;
}
.alc-badge-critico   { background: #e74c3c; }
.alc-badge-moderado  { background: #f39c12; }
.alc-badge-informativo { background: #3498db; }

/* Indicadores de severidade (emojis via content) */
.alc-sev { font-weight: 700; margin-right: 6px; }

/* Nível: Type 05 */
.alc-t05 {
    font-size: 1.05em; font-weight: 800;
    margin: 10px 0 4px 0; padding: 2px 0;
}

/* Nível: Type 06 */
.alc-t06 {
    font-size: 0.95em; font-weight: 700;
    padding: 4px 0 2px 20px;
}
.alc-t06-vals {
    font-size: 0.82em; font-weight: 400; opacity: 0.85;
    padding: 0 0 2px 20px;
}

/* Barra de intensidade */
.alc-bar-wrap {
    height: 8px; background: #ececec; border-radius: 4px;
    margin: 2px 0 4px 20px; max-width: 380px; overflow: hidden;
    border: 1px solid rgba(0, 0, 0, 0.06);
}
.alc-bar {
    height: 100%; border-radius: 4px; min-width: 4px;
    transition: width 0.3s ease;
}
.alc-bar-critico   { background: linear-gradient(90deg, #e74c3c, #c0392b); }
.alc-bar-moderado  { background: linear-gradient(90deg, #f39c12, #e67e22); }
.alc-bar-informativo { background: linear-gradient(90deg, #3498db, #2980b9); }

/* Nível: Account */
.alc-acc {
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: 0.88em; padding: 2px 0 1px 40px; color: #555;
}
.alc-acc-credit { font-style: italic; }

/* Nível: Oficina */
.alc-ofi {
    padding: 1px 0 1px 60px; font-size: 0.84em;
}
.alc-ofi-name { font-weight: 600; }

/* Nível: Texto breve */
.alc-txt {
    padding: 0 0 0 80px; font-size: 0.80em; color: #777;
    font-family: 'Consolas', 'Courier New', monospace;
}

/* Rodapé */
.alc-footer {
    margin-top: 10px; padding-top: 8px;
    border-top: 2px solid #e0e0e0;
}
.alc-meta { font-size: 0.82em; color: #888; font-weight: 600; }
.alc-legenda { font-size: 0.75em; color: #999; margin-top: 4px; }
.alc-bar-note { font-size: 0.74em; color: #999; margin-top: 4px; }

/* Tree lines */
.alc-tree { color: #bbb; font-family: monospace; }
</style>
"""


# =========================================================================
#  Página principal
# =========================================================================

def render_monitoring_page() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
    st.header("🔔 Central de Alertas")

    rules_data = load_alert_rules()
    rules = rules_data.get("rules", [])

    if not rules:
        st.info("Nenhuma regra de alerta configurada. Acesse a página de Configuração.")
        return

    # --- Seletores: Ano + Período ---
    hoje = date.today()
    anos_regras = sorted({r.get("ano", hoje.year) for r in rules}, reverse=True)
    col_ano, col_per, col_btn = st.columns([1, 2, 2])

    with col_ano:
        ano_sel = st.selectbox("Ano", anos_regras, key="al_mon_ano")

    with col_per:
        periodos = periodos_disponiveis(ano_sel)
        mes_default = mes_atual_nome(hoje)
        idx_default = periodos.index(mes_default) if mes_default in periodos else 0
        periodo_sel = st.selectbox(
            "Período", periodos, index=idx_default, key="al_mon_per",
        ) if periodos else None

    if not periodo_sel:
        st.warning("Sem dados disponíveis para o ano selecionado.")
        return

    # --- Carregar dados e calcular ---
    with col_btn:
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            run_btn = st.button("▶ Verificar agora", type="primary")
        with btn_col2:
            run_all_btn = st.button("🔔 Disparar alertas ativos")

    if run_all_btn:
        with st.spinner("Disparando todos os alertas ativos..."):
            from alertas.alert_engine import run_daily_check
            logs = run_daily_check(periodo=periodo_sel, data_ref=hoje)

        if logs:
            enviados = sum(
                1 for item in logs
                if item.get("notificacoes_enviadas", {}).get("email")
                or item.get("notificacoes_enviadas", {}).get("teams")
            )
            st.success(
                f"{len(logs)} alerta(s) ativo(s) processado(s); "
                f"{enviados} notificação(ões) enviada(s)."
            )
        else:
            st.info("Nenhum alerta ativo gerou desvio para o período selecionado.")

    if run_btn:
        with st.spinner("Calculando..."):
            data = load_all_data(ano_sel)
            prop = proporcao_mes(hoje)

            # Pegar primeira regra ativa para obter config
            regras_ativas = [
                r for r in rules
                if r.get("ativo") and r.get("ano") == ano_sel
            ]
            if not regras_ativas:
                st.info("Nenhuma regra ativa para este ano.")
                return

            regra = regras_ativas[0]
            modo = regra.get("modo_comparacao", "flex_bud_x_real")
            top_n = regra.get("top_n", 10)
            moeda = regra.get("moeda", "BRL")

            ranking = calcular_ranking_consolidado(
                data=data,
                periodo=periodo_sel,
                modo=modo,
                proporcao=prop,
                top_n=top_n,
                filtro_type_05=regra.get("filtro_type_05"),
                filtro_type_06=regra.get("filtro_type_06"),
                filtro_account=regra.get("filtro_account"),
                moeda=moeda,
            )

        if not ranking:
            st.success("✅ Nenhum desvio significativo encontrado.")
        else:
            append_to_alert_log(
                build_alert_log_entry(
                    ranking,
                    periodo=periodo_sel,
                    ano=ano_sel,
                    modo=modo,
                    proporcao=prop,
                    rule_id=regra.get("id", ""),
                    titulo=f"Previa Central de Alertas - {periodo_sel}",
                    tipo="manual",
                    metadata_extra={
                        "fonte": "central_alertas",
                    },
                )
            )

            # Gerar tabela de validação para enviar junto
            tabela_df = gerar_tabela_validacao(
                data=data,
                oficina=None,
                periodo=periodo_sel,
                proporcao=prop,
                moeda=moeda,
                filtro_type_05=regra.get("filtro_type_05") or None,
                filtro_type_06=regra.get("filtro_type_06") or None,
                filtro_account=regra.get("filtro_account") or None,
            )

            # Enviar notificações
            config = rules_data.get("config", {})
            notif = config.get("notifications_enabled", {})

            if notif.get("teams"):
                webhook = config.get("teams_webhook_url", "")
                if webhook:
                    from alertas.notifications_teams import send_alert_teams_consolidated
                    try:
                        send_alert_teams_consolidated(
                            ranking, webhook, modo, tabela_df,
                        )
                        st.toast("📤 Alerta enviado para o Teams")
                    except Exception as e:
                        st.warning(f"⚠️ Falha ao enviar Teams: {e}")

            if notif.get("email"):
                email_cfg = config.get("email", {})
                if email_cfg.get("recipients"):
                    from alertas.notifications_email import send_alert_email_consolidated
                    try:
                        send_alert_email_consolidated(
                            ranking, email_cfg, modo, prop, tabela_df,
                        )
                        st.toast("📧 Alerta enviado por e-mail")
                    except Exception as e:
                        st.warning(f"⚠️ Falha ao enviar e-mail: {e}")

            # Renderizar card consolidado
            modo_label = MODOS_COMPARACAO.get(modo, modo)
            _render_card_consolidado(ranking, modo_label, prop)
            _render_resumo_top_validacao(tabela_df, top_n)

    # --- Tabela de validação (sempre visível) ---
    _render_tabela_validacao(ano_sel, periodo_sel, hoje)

    # --- Mensagens internas (histórico recente) ---
    _render_mensagens_recentes()


# =========================================================================
#  Card Consolidado (drill-down hierárquico)
# =========================================================================

def _sev_icons(pct: float, desvio: float | None = None) -> str:
    """Retorna ícones de severidade baseados no % de desvio."""
    if desvio is not None and desvio <= 0:
        return "🟢"
    pct = abs(pct)
    if pct > 50:
        return "🔴🔴🔴"
    if pct > 15:
        return "🔴🔴"
    if pct > 5:
        return "🟠"
    if pct > 1:
        return "🟡"
    return "🟢"


def _sev_class(pct: float, desvio: float | None = None) -> str:
    if desvio is not None and desvio <= 0:
        return "informativo"
    pct = abs(pct)
    if pct > 15:
        return "critico"
    if pct > 5:
        return "moderado"
    return "informativo"


def _bar_html(desvio: float, total_desvio_abs: float, sev_cls: str) -> str:
    """Barra de intensidade proporcional ao desvio absoluto total exibido."""
    if total_desvio_abs <= 0:
        return ""
    pct = min(100.0, abs(desvio) / total_desvio_abs * 100)
    return (
        f'<div class="alc-bar-wrap">'
        f'<div class="alc-bar alc-bar-{sev_cls}" style="width:{pct:.0f}%"></div>'
        f'</div>'
    )


def _bar_pct(desvio: float, total_desvio_abs: float) -> float:
    """Percentual da barra em relacao ao desvio absoluto total exibido."""
    if total_desvio_abs <= 0:
        return 0.0
    return min(100.0, abs(desvio) / total_desvio_abs * 100)


def _fmt_k_sign(valor: float, moeda: str) -> str:
    """Valor em kMOEDA com sinal."""
    v = valor / 1000 if valor else 0.0
    sinal = "+" if v >= 0 else ""
    # pt-BR: ponto milhar, vírgula decimal
    s = f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{sinal}{s} k{moeda}"


def _fmt_cpu_sign(valor: float, simbolo: str) -> str:
    v = valor if valor else 0.0
    sinal = "+" if v >= 0 else ""
    s = f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"Δ {sinal}{s} {simbolo}/veíc"


def _render_card_consolidado(
    ranking: dict, modo_label: str, proporcao: float,
) -> None:
    """Renderiza card consolidado com drill-down hierárquico."""
    periodo = ranking.get("periodo", "")
    moeda = ranking.get("moeda", "BRL")
    simbolo = ranking.get("simbolo", "R$")
    sev = ranking.get("severidade", "informativo")
    total_desvio = ranking.get("total_desvio", 0)
    itens = ranking.get("itens", [])

    # Base das barras: participacao de cada Type 06 no desvio absoluto total exibido.
    total_desvio_abs = sum(abs(it.get("desvio", 0)) for it in itens)

    # --- Agrupar itens por Type 05 ---
    by_t05: dict[str, list[dict]] = {}
    for it in itens:
        t05 = it.get("type_05", "Outros")
        by_t05.setdefault(t05, []).append(it)

    # --- Montar HTML ---
    lines: list[str] = []

    # Header
    lines.append('<div class="alc-card">')
    lines.append('<div class="alc-header">')
    lines.append(f'<div><div class="alc-title">📊 Relatório de Alertas — {_esc(periodo)}</div>')
    lines.append(f'<div class="alc-subtitle">{_esc(modo_label)} · P = {proporcao:.1%} · Moeda: {_esc(moeda)}</div>')
    lines.append('</div>')
    lines.append(f'<span class="alc-badge alc-badge-{sev}">{sev.upper()}</span>')
    lines.append('</div>')

    # Body: itens agrupados por Type 05
    for t05 in by_t05:
        t05_itens = by_t05[t05]
        t05_desvio = sum(it["desvio"] for it in t05_itens)
        t05_pct = sum(it["desvio_pct"] for it in t05_itens) / len(t05_itens) if t05_itens else 0
        t05_icons = _sev_icons(t05_pct, t05_desvio)

        lines.append(
            f'<div class="alc-t05">{t05_icons} {_esc(t05)}</div>'
        )

        for idx_t6, it in enumerate(t05_itens):
            t6 = it["type_06"]
            sev_cls = _sev_class(it["desvio_pct"], it["desvio"])
            icons = _sev_icons(it["desvio_pct"], it["desvio"])
            is_last_t6 = idx_t6 == len(t05_itens) - 1
            tree_t6 = "└─" if is_last_t6 else "├─"

            lines.append(
                f'<div class="alc-t06">'
                f'<span class="alc-tree">{tree_t6}</span> '
                f'{icons} {_esc(t6)}: '
                f'{_esc(fmt_k(it["real"], moeda))} '
                f'({_esc(fmt_k(it["cpu_real"] * 1000, simbolo, 1).replace("k" + simbolo, simbolo + "/veíc"))})'
                f'</div>'
            )

            # Barra + delta
            fill_pct = _bar_pct(it["desvio"], total_desvio_abs)
            lines.append(
                f'<div class="alc-t06-vals">'
                f'{_bar_html(it["desvio"], total_desvio_abs, sev_cls)}'
                f'<span style="margin-left:20px;">'
                f'{_esc(fmt_delta_k(it["desvio"], moeda))} · '
                f'{abs(it["desvio_pct"]):.1f}% · '
                f'representa {fill_pct:.0f}% do desvio total</span>'
                f'</div>'
            )

            # Accounts
            accounts = it.get("accounts", [])
            tree_cont = "│  " if not is_last_t6 else "   "

            for idx_acc, acc in enumerate(accounts):
                is_last_acc = idx_acc == len(accounts) - 1
                tree_acc = "└─" if is_last_acc else "├─"
                credit_tag = ""
                if acc.get("esperado", 0) < 0:
                    credit_tag = ' <span style="color:#e67e22;font-size:0.85em;">⚠️ menos receita</span>'

                lines.append(
                    f'<div class="alc-acc">'
                    f'<span class="alc-tree">{tree_cont}{tree_acc}</span> '
                    f'{_esc(acc["account"])}: '
                    f'{_esc(_fmt_k_sign(acc["desvio"], moeda))} '
                    f'({_esc(_fmt_cpu_sign(acc["delta_cpu"], simbolo))})'
                    f'{credit_tag}'
                    f'</div>'
                )

                # Oficinas
                oficinas = acc.get("oficinas", [])
                tree_cont_acc = tree_cont + ("│  " if not is_last_acc else "   ")

                for idx_ofi, ofi in enumerate(oficinas):
                    is_last_ofi = idx_ofi == len(oficinas) - 1
                    tree_ofi = "└─" if is_last_ofi else "├─"

                    lines.append(
                        f'<div class="alc-ofi">'
                        f'<span class="alc-tree">{tree_cont_acc}{tree_ofi}</span> '
                        f'{_esc(fmt_linha_oficina(ofi, moeda, simbolo).strip())}'
                        f'</div>'
                    )

                    # Textos breves
                    textos = ofi.get("textos", [])
                    tree_cont_ofi = tree_cont_acc + ("│  " if not is_last_ofi else "   ")

                    for txt in textos:
                        tv = txt["valor"] / 1000 if txt["valor"] else 0
                        tv_fmt = f"{tv:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        lines.append(
                            f'<div class="alc-txt">'
                            f'<span class="alc-tree">{tree_cont_ofi}</span> '
                            f'• {_esc(txt["texto"])}  '
                            f'<span style="opacity:0.7;">({tv_fmt}k)</span>'
                            f'</div>'
                        )

    # Footer
    lines.append('<div class="alc-footer">')
    lines.append(
        f'<div class="alc-meta">📈 Desvio Total: '
        f'{_esc(_fmt_k_sign(total_desvio, moeda))}</div>'
    )
    lines.append(
        '<div class="alc-legenda">'
        '🔴🔴🔴 &gt;50% · 🔴🔴 15-50% · 🟠 5-15% · 🟡 1-5% · 🟢 &lt;1%'
        '</div>'
    )
    lines.append(
        '<div class="alc-bar-note">'
        'Barra = participacao do desvio absoluto de cada Type 06 sobre o desvio absoluto total exibido neste card.'
        '</div>'
    )
    lines.append('</div>')
    lines.append('</div>')  # fecha alc-card

    st.markdown("\n".join(lines), unsafe_allow_html=True)


def _esc(text: str) -> str:
    """Escape HTML básico."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _render_resumo_top_validacao(
    tabela_df: pd.DataFrame,
    top_n: int,
) -> None:
    """Mostra um resumo enxuto logo abaixo do card principal."""
    if tabela_df is None or tabela_df.empty:
        return

    st.markdown(f"#### 📋 Resumo de Validação (Top {top_n})")
    resumo = tabela_df.head(top_n).copy()
    resumo = resumo[
        [
            "Type 05",
            "Type 06",
            "Account",
            "Real",
            "Real - Flex BUD P",
            "% Delta",
        ]
    ].rename(
        columns={
            "Real": "Realizado",
            "Real - Flex BUD P": "Delta",
        }
    )
    st.dataframe(resumo, width="stretch", hide_index=True)


# =========================================================================
#  Tabela de validação (conferência de valores)
# =========================================================================

def _render_tabela_validacao(
    ano: int, periodo: str, hoje: date,
) -> None:
    """Tabela com valores detalhados para conferência."""
    st.subheader("📊 Tabela de Validação")

    # --- Linha 1: Oficina + Moeda ---
    oficinas = oficinas_disponiveis(ano)
    ofi_options = ["Todas"] + oficinas

    col_ofi, col_moeda = st.columns([2, 1])
    with col_ofi:
        ofi_sel = st.selectbox(
            "Oficina", ofi_options, key="al_val_ofi",
        )
    with col_moeda:
        moeda_sel = st.selectbox(
            "Moeda", ["BRL", "EUR", "USD"], key="al_val_moeda",
        )

    # --- Filtros cascata Type 05 → Type 06 → Account ---
    # Calcular opções disponíveis com cascata
    t05_opts = type05_disponiveis(ano)

    # Ler seleção atual de Type 05
    f_type05_current = [
        v for v in st.session_state.get("al_val_t05", [])
        if v in t05_opts
    ]

    # Opções de Type 06 filtradas pelo Type 05 selecionado
    t06_opts = type06_disponiveis(
        ano, f_type05_current or None,
    )

    # Limpar Type 06 inválidos ANTES de criar o widget
    if "al_val_t06" in st.session_state:
        cleaned = [v for v in st.session_state["al_val_t06"]
                   if v in t06_opts]
        st.session_state["al_val_t06"] = cleaned

    f_type06_current = [
        v for v in st.session_state.get("al_val_t06", [])
        if v in t06_opts
    ]

    # Opções de Account filtradas pelo Type 06 selecionado
    acc_opts = accounts_disponiveis(
        ano, f_type06_current or None,
    )

    # Limpar Account inválidos ANTES de criar o widget
    if "al_val_acc" in st.session_state:
        cleaned = [v for v in st.session_state["al_val_acc"]
                   if v in acc_opts]
        st.session_state["al_val_acc"] = cleaned

    # Agora criar os widgets
    col_t05, col_t06, col_acc = st.columns(3)
    with col_t05:
        f_type05 = st.multiselect(
            "Type 05", t05_opts, key="al_val_t05",
        )
    with col_t06:
        f_type06 = st.multiselect(
            "Type 06", t06_opts, key="al_val_t06",
        )
    with col_acc:
        f_account = st.multiselect(
            "Account", acc_opts, key="al_val_acc",
        )

    # --- Carregar dados e gerar tabela ---
    oficina_param = None if ofi_sel == "Todas" else ofi_sel
    prop = proporcao_mes(hoje)

    with st.spinner("Carregando dados..."):
        data = load_all_data(ano)

    df = gerar_tabela_validacao(
        data=data,
        oficina=oficina_param,
        periodo=periodo,
        proporcao=prop,
        moeda=moeda_sel,
        filtro_type_05=f_type05 or None,
        filtro_type_06=f_type06 or None,
        filtro_account=f_account or None,
    )

    if df.empty:
        st.info("Sem dados para os filtros selecionados.")
        return

    st.caption(
        f"Período: **{periodo}** · Proporção (P): **{prop:.1%}** · "
        f"Moeda: **{moeda_sel}** · "
        f"Oficina: **{ofi_sel}** · "
        f"Linhas: **{len(df)}**"
    )

    # Colorir coluna de delta
    def _color_delta(val: float) -> str:
        if val > 0:
            return "color: #e74c3c"
        if val < 0:
            return "color: #27ae60"
        return ""

    styled = df.style.format(
        {
            "Flex BUD": "{:,.2f}",
            "Flex BUD P": "{:,.2f}",
            "Real": "{:,.2f}",
            "Real - Flex BUD P": "{:,.2f}",
            "% Delta": "{:,.2f}%",
        },
    ).map(_color_delta, subset=["Real - Flex BUD P", "% Delta"])

    st.dataframe(styled, width="stretch", height=600)


# =========================================================================
#  Mensagens internas recentes
# =========================================================================

def _render_mensagens_recentes() -> None:
    st.subheader("📬 Ultimos 10 Textos Gerados")
    log = load_alert_log()
    if not log:
        st.info("Nenhum texto gerado no historico.")
        return

    recentes = log[::-1]
    for al in recentes:
        sev = al.get("severidade", "informativo")
        emoji = {"critico": "🔴", "moderado": "🟡"}.get(sev, "🔵")
        titulo = al.get("titulo", "")
        ts = al.get("timestamp", "")
        tipo = al.get("tipo", "automatico")
        texto = al.get("generated_texts", {}).get("plain_text_tree") or al.get("mensagem", "")
        with st.expander(f"{emoji} {titulo} - {ts} [{tipo}]"):
            st.text(texto)


# =========================================================================
#  Execução (Streamlit st.Page)
# =========================================================================

render_monitoring_page()
