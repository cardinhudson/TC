"""Notificações Microsoft Teams — formato relatório."""

from __future__ import annotations

import json
import ssl
import urllib.request

import certifi
import pandas as pd
import requests
import truststore

from alertas.alert_engine import (
    MODOS_COMPARACAO,
    fmt_delta_k,
    fmt_k,
    fmt_linha_account,
    fmt_linha_oficina,
    fmt_linha_type06,
)


# =========================================================================
#  Helpers de formatação para texto Teams
# =========================================================================

def _sev_icons(pct: float, desvio: float | None = None) -> str:
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


def _bar_text(desvio: float, total_desvio_abs: float, width: int = 10) -> str:
    """Barra textual proporcional ao desvio absoluto total do card."""
    if total_desvio_abs <= 0:
        return "[░░░░░░░░░░] representa 0% do desvio total"

    ratio = min(1.0, abs(desvio) / total_desvio_abs)
    filled = int(round(ratio * width))
    if abs(desvio) > 0 and filled == 0:
        filled = 1
    empty = max(0, width - filled)
    return f"[{'█' * filled}{'░' * empty}] representa {ratio * 100:.0f}% do desvio total"


def _tree_html(text: str) -> str:
    """Renderiza os conectores da arvore em cinza para melhorar a leitura."""
    text_html = text.replace(" ", "&nbsp;")
    return f'<span style="color:#9a9a9a;font-family:Consolas,monospace;">{text_html}</span>'


def _build_ranking_text(ranking: dict) -> str:
    """Monta texto hierárquico completo do ranking (formato árvore)."""
    moeda = ranking.get("moeda", "BRL")
    simbolo = ranking.get("simbolo", "R$")
    itens = ranking.get("itens", [])
    total_desvio_abs = sum(abs(it.get("desvio", 0)) for it in itens)

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
        t05_desvio = sum(it.get("desvio", 0) for it in t05_itens)
        lines.append(f"<strong>{_sev_icons(t05_pct, t05_desvio)} {t05}</strong>")

        for idx_t6, it in enumerate(t05_itens):
            is_last_t6 = idx_t6 == len(t05_itens) - 1
            tree_t6 = "└─" if is_last_t6 else "├─"
            icons = _sev_icons(it["desvio_pct"], it.get("desvio", 0))

            lines.append(
                f"{_tree_html(tree_t6)} <strong>{icons} {it['type_06']}:</strong> "
                f"{fmt_k(it['real'], moeda)}"
            )

            tree_cont = "│  " if not is_last_t6 else "   "

            lines.append(
                f"{_tree_html(tree_cont)} {_bar_text(it['desvio'], total_desvio_abs)} · "
                f"{fmt_delta_k(it['desvio'], moeda)} · "
                f"{abs(it['desvio_pct']):.1f}%"
            )

            accounts = it.get("accounts", [])

            for idx_acc, acc in enumerate(accounts):
                is_last_acc = idx_acc == len(accounts) - 1
                tree_acc = "└─" if is_last_acc else "├─"
                credit_tag = ""
                if acc.get("esperado", 0) < 0:
                    credit_tag = " ⚠️ menos receita"

                lines.append(
                    f"{_tree_html(tree_cont + tree_acc)} <strong>{acc['account']}:</strong> "
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
                        f"{_tree_html(tree_cont_acc + tree_ofi)} "
                        f"{fmt_linha_oficina(ofi, moeda, simbolo).strip()}"
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
                            f"{_tree_html(tree_cont_ofi + '  ')} "
                            f"• {txt['texto']}  ({tv_fmt}k)"
                        )

    return "<br>".join(lines)


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
        f"🟢{_sp}&lt;1%<br>"
        f"Barra{_sp}={_sp}desvio{_sp}absoluto{_sp}de{_sp}cada{_sp}Type{_sp}06{_sp}"
        f"como{_sp}percentual{_sp}do{_sp}desvio{_sp}absoluto{_sp}total{_sp}do{_sp}card"
    )

    resumo_top_10 = ""
    if tabela_df is not None and not tabela_df.empty:
        top_df = tabela_df.head(10)
        linhas_top: list[str] = []
        for _, row in top_df.iterrows():
            t05 = row.get("Type 05", "")
            t06 = row.get("Type 06", "")
            acc = row.get("Account", "")
            real = row.get("Real", 0)
            delta = row.get("Real - Flex BUD P", 0)
            pct = row.get("% Delta", 0)
            linhas_top.append(
                "<strong>"
                f"{t05} / {t06} / {acc}"
                "</strong>: "
                f"Realizado {fmt_k(real, moeda)} · "
                f"{fmt_delta_k(delta, moeda)} · "
                f"{pct:+.1f}%"
            )
        if linhas_top:
            resumo_top_10 = (
                "<br><br><strong>📋 Tabela de Validação (Top 10)</strong><br>"
                + "<br>".join(linhas_top)
            )

    sections = [
        {
            "activityTitle": f"📊 Relatório de Alertas — {periodo}",
            "activitySubtitle": (
                f"{modo_label} · Severidade: {sev.upper()} · Moeda: {moeda}"
            ),
            "text": ranking_text + footer + resumo_top_10,
            "markdown": False,
        },
    ]

    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": cor,
        "summary": f"SCI Alerta — {periodo}",
        "sections": sections,
    }


def _build_test_ranking() -> dict:
    """Ranking demonstrativo para validar o visual do card no Teams."""
    return {
        "periodo": "Preview Visual",
        "moeda": "BRL",
        "simbolo": "R$",
        "severidade": "critico",
        "total_desvio": 184000.0,
        "itens": [
            {
                "type_05": "Burden",
                "type_06": "Material Losses",
                "real": 412000.0,
                "esperado": 288000.0,
                "desvio": 124000.0,
                "desvio_pct": 43.1,
                "delta_cpu": 24.8,
                "accounts": [
                    {
                        "account": "Scrap Sales",
                        "desvio": 104200.0,
                        "delta_cpu": 20.8,
                        "esperado": 50000.0,
                        "oficinas": [
                            {
                                "oficina": "OF1",
                                "desvio": 104200.0,
                                "delta_cpu": 20.8,
                                "textos": [
                                    {"texto": 'tubo aco s/ costura sch 40 3/4"', "valor": 104200.0},
                                    {"texto": "tubo metalon 40 x 40 x 3,0 stalx", "valor": 57400.0},
                                    {"texto": "tubo metalon 50 x 50 x 3,0 stalx", "valor": 40800.0},
                                ],
                            }
                        ],
                    }
                ],
            },
            {
                "type_05": "Labor",
                "type_06": "Direct Labor",
                "real": 298000.0,
                "esperado": 238000.0,
                "desvio": 60000.0,
                "desvio_pct": 25.2,
                "delta_cpu": 12.0,
                "accounts": [
                    {
                        "account": "Wages",
                        "desvio": 60000.0,
                        "delta_cpu": 12.0,
                        "esperado": 238000.0,
                        "oficinas": [
                            {
                                "oficina": "OF2",
                                "desvio": 60000.0,
                                "delta_cpu": 12.0,
                                "textos": [
                                    {"texto": "horas extras manutencao", "valor": 32100.0},
                                    {"texto": "apoio setup linha", "valor": 17900.0},
                                ],
                            }
                        ],
                    }
                ],
            },
        ],
    }


def _build_test_validation_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Type 06": "Material Losses",
                "Account": "Scrap Sales",
                "Real - Flex BUD P": 104200.0,
                "% Delta": 43.1,
            },
            {
                "Type 06": "Direct Labor",
                "Account": "Wages",
                "Real - Flex BUD P": 60000.0,
                "% Delta": 25.2,
            },
            {
                "Type 06": "Energy",
                "Account": "Electricity",
                "Real - Flex BUD P": 19800.0,
                "% Delta": 7.9,
            },
        ]
    )


def _build_ssl_contexts() -> list[ssl.SSLContext]:
    """Monta contexts de SSL em ordem de prioridade.

    1. Repositório nativo do Windows via truststore.
    2. Contexto padrão do Python/Windows.
    3. Fallback explícito com o bundle do certifi.
    """
    contexts: list[ssl.SSLContext] = []

    try:
        contexts.append(truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT))
    except Exception:
        pass

    contexts.append(ssl.create_default_context())

    try:
        certifi_ctx = ssl.create_default_context(cafile=certifi.where())
        contexts.append(certifi_ctx)
    except Exception:
        pass

    return contexts


def _post_teams_card(webhook_url: str, card: dict) -> None:
    """Envia o card ao Teams tentando primeiro a confianca padrão do sistema."""
    payload = json.dumps(card).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    errors: list[str] = []

    try:
        truststore.inject_into_ssl()
        resp = requests.post(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        return
    except Exception as exc:
        errors.append(f"requests(truststore): {exc}")

    for idx, context in enumerate(_build_ssl_contexts(), start=1):
        try:
            with urllib.request.urlopen(
                req,
                timeout=30,
                context=context,
            ) as resp:
                resp.read()
            return
        except Exception as exc:
            errors.append(f"urllib(context={idx}): {exc}")

    raise RuntimeError(" ; ".join(errors))


def send_test_teams_card(webhook_url: str) -> None:
    """Envia um card de preview com a mesma estrutura visual do alerta consolidado."""
    ranking = _build_test_ranking()
    tabela_df = _build_test_validation_table()
    card = build_teams_card_consolidated(ranking, "flex_bud_x_real", tabela_df)
    card["summary"] = "SCI — Preview visual do alerta"
    card["sections"][0]["activityTitle"] = "🧪 Preview Visual — Alerta SCI"
    card["sections"][0]["activitySubtitle"] = (
        "Simulação de webhook para validar hierarquia, barra, cores e legibilidade"
    )

    _post_teams_card(webhook_url, card)


def send_alert_teams(alerta: dict, webhook_url: str) -> None:
    """POST MessageCard para webhook Teams."""
    card = build_teams_card(alerta)
    _post_teams_card(webhook_url, card)


def send_alert_teams_consolidated(
    ranking: dict, webhook_url: str, modo: str,
    tabela_df: pd.DataFrame | None = None,
) -> None:
    """POST MessageCard consolidado para webhook Teams."""
    card = build_teams_card_consolidated(ranking, modo, tabela_df)
    _post_teams_card(webhook_url, card)
