"""Notificações Microsoft Teams — formato relatório."""

from __future__ import annotations

import json
import urllib.request

import pandas as pd

from alertas.alert_engine import (
    MODOS_COMPARACAO,
    fmt_delta_k,
    fmt_k,
    fmt_linha_account,
    fmt_linha_type06,
)


# =========================================================================
#  Helpers de formatação para texto Teams
# =========================================================================

def _sev_icons(pct: float) -> str:
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


def _fmt_k_sign(valor: float, moeda: str) -> str:
    v = valor / 1000 if valor else 0.0
    sinal = "+" if v >= 0 else ""
    s = f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{sinal}{s} k{moeda}"


def _fmt_cpu_sign(valor: float, simbolo: str) -> str:
    v = valor if valor else 0.0
    sinal = "+" if v >= 0 else ""
    s = f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"Δ {sinal}{s} {simbolo}/veíc"


def _build_ranking_text(ranking: dict) -> str:
    """Monta texto hierárquico completo do ranking (formato árvore)."""
    moeda = ranking.get("moeda", "BRL")
    simbolo = ranking.get("simbolo", "R$")
    itens = ranking.get("itens", [])

    # Agrupar por Type 05
    by_t05: dict[str, list[dict]] = {}
    for it in itens:
        t05 = it.get("type_05", "Outros")
        by_t05.setdefault(t05, []).append(it)

    lines: list[str] = []

    for t05, t05_itens in by_t05.items():
        t05_pct = (
            sum(it["desvio_pct"] for it in t05_itens) / len(t05_itens)
            if t05_itens else 0
        )
        lines.append(f"{_sev_icons(t05_pct)} {t05}")

        for idx_t6, it in enumerate(t05_itens):
            is_last_t6 = idx_t6 == len(t05_itens) - 1
            tree_t6 = "└─" if is_last_t6 else "├─"
            icons = _sev_icons(it["desvio_pct"])

            lines.append(
                f"{tree_t6} {icons} {it['type_06']}: "
                f"{fmt_k(it['real'], moeda)} "
                f"| {fmt_delta_k(it['desvio'], moeda)} · "
                f"{abs(it['desvio_pct']):.1f}%"
            )

            accounts = it.get("accounts", [])
            tree_cont = "│  " if not is_last_t6 else "   "

            for idx_acc, acc in enumerate(accounts):
                is_last_acc = idx_acc == len(accounts) - 1
                tree_acc = "└─" if is_last_acc else "├─"
                credit_tag = ""
                if acc.get("esperado", 0) < 0:
                    credit_tag = " ⚠️ menos receita"

                lines.append(
                    f"{tree_cont}{tree_acc} {acc['account']}: "
                    f"{_fmt_k_sign(acc['desvio'], moeda)} "
                    f"({_fmt_cpu_sign(acc['delta_cpu'], simbolo)})"
                    f"{credit_tag}"
                )

                oficinas = acc.get("oficinas", [])
                tree_cont_acc = tree_cont + ("│  " if not is_last_acc else "   ")

                for idx_ofi, ofi in enumerate(oficinas):
                    is_last_ofi = idx_ofi == len(oficinas) - 1
                    tree_ofi = "└─" if is_last_ofi else "├─"

                    lines.append(
                        f"{tree_cont_acc}{tree_ofi} "
                        f"📍 {ofi['oficina']}: "
                        f"{_fmt_k_sign(ofi['desvio'], moeda)}"
                    )

                    textos = ofi.get("textos", [])
                    tree_cont_ofi = tree_cont_acc + (
                        "│  " if not is_last_ofi else "   "
                    )

                    for txt in textos:
                        tv = txt["valor"] / 1000 if txt["valor"] else 0
                        tv_fmt = (
                            f"{tv:,.1f}"
                            .replace(",", "X")
                            .replace(".", ",")
                            .replace("X", ".")
                        )
                        lines.append(
                            f"{tree_cont_ofi}  "
                            f"• {txt['texto']}  ({tv_fmt}k)"
                        )

    return "<br>".join(l.replace(" ", "&nbsp;") for l in lines)


def build_teams_card(alerta: dict) -> dict:
    """Monta MessageCard (Office 365 connector) com ranking no formato relatório."""
    sev = alerta.get("severidade", "informativo")
    cor = {"critico": "FF0000", "moderado": "FFA500", "informativo": "3498DB"}.get(sev, "3498DB")

    meta = alerta.get("metadata", {})
    ranking = alerta.get("ranking", {})
    moeda = meta.get("moeda", "BRL")
    simbolo = ranking.get("simbolo", "R$")

    # Montar facts com formato relatório
    facts: list[dict] = []
    for it in ranking.get("itens", [])[:8]:
        facts.append({
            "name": it.get("type_06", ""),
            "value": fmt_linha_type06(it, moeda, simbolo),
        })
        for acc in it.get("accounts", [])[:5]:
            facts.append({
                "name": "",
                "value": fmt_linha_account(acc, moeda, simbolo),
            })

    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": cor,
        "summary": alerta.get("titulo", "SCI Alerta"),
        "sections": [
            {
                "activityTitle": alerta.get("titulo", ""),
                "activitySubtitle": (
                    f"{meta.get('modo_label', '')} · "
                    f"P = {meta.get('proporcao_mes', 0):.1%}"
                ),
                "facts": facts,
                "markdown": True,
            },
        ],
    }


def build_teams_card_consolidated(
    ranking: dict, modo: str,
    tabela_df: pd.DataFrame | None = None,
) -> dict:
    """Monta MessageCard para ranking consolidado (card único com árvore)."""
    sev = ranking.get("severidade", "informativo")
    cor = {"critico": "FF0000", "moderado": "FFA500", "informativo": "3498DB"}.get(sev, "3498DB")
    moeda = ranking.get("moeda", "BRL")
    periodo = ranking.get("periodo", "")
    total_desvio = ranking.get("total_desvio", 0)
    modo_label = MODOS_COMPARACAO.get(modo, modo)

    # Texto hierárquico completo (árvore) — já em HTML (<br> + &nbsp;)
    ranking_text = _build_ranking_text(ranking)

    # Footer (HTML)
    _sp = "&nbsp;"
    footer = (
        f"<br><br>📈{_sp}Desvio{_sp}Total:{_sp}"
        f"{_fmt_k_sign(total_desvio, moeda).replace(' ', _sp)}<br>"
        f"🔴🔴🔴{_sp}&gt;50%{_sp}·{_sp}"
        f"🔴🔴{_sp}15-50%{_sp}·{_sp}"
        f"🟠{_sp}5-15%{_sp}·{_sp}"
        f"🟡{_sp}1-5%{_sp}·{_sp}"
        f"🟢{_sp}&lt;1%"
    )

    sections = [
        {
            "activityTitle": f"📊 Relatório de Alertas — {periodo}",
            "activitySubtitle": (
                f"{modo_label} · Severidade: {sev.upper()} · Moeda: {moeda}"
            ),
            "text": ranking_text + footer,
            "markdown": False,
        },
    ]

    # Seção 2: resumo da tabela de validação (top 10)
    if tabela_df is not None and not tabela_df.empty:
        tab_lines: list[str] = []
        for _, row in tabela_df.head(10).iterrows():
            t06 = row.get("Type 06", "")
            acc = row.get("Account", "")
            delta = row.get("Real - Flex BUD P", 0)
            pct = row.get("% Delta", 0)
            tab_lines.append(f"{t06} / {acc}: ∆ {delta:+,.0f} ({pct:+.1f}%)")
        if tab_lines:
            sections.append({
                "activityTitle": "📋 Tabela de Validação (Top 10)",
                "text": "<br>".join(
                    l.replace(" ", "&nbsp;") for l in tab_lines
                ),
                "markdown": False,
            })

    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": cor,
        "summary": f"SCI Alerta — {periodo}",
        "sections": sections,
    }


def send_alert_teams(alerta: dict, webhook_url: str) -> None:
    """POST MessageCard para webhook Teams."""
    card = build_teams_card(alerta)
    payload = json.dumps(card).encode("utf-8")

    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


def send_alert_teams_consolidated(
    ranking: dict, webhook_url: str, modo: str,
    tabela_df: pd.DataFrame | None = None,
) -> None:
    """POST MessageCard consolidado para webhook Teams."""
    card = build_teams_card_consolidated(ranking, modo, tabela_df)
    payload = json.dumps(card).encode("utf-8")

    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()
