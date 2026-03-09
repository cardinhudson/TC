"""Notificações por e-mail — formato relatório consolidado + tabela.

Transporte via Microsoft Graph API (OAuth 2.0 Device Code Flow).
"""

from __future__ import annotations

import logging

import pandas as pd

from alertas.alert_engine import (
    MODOS_COMPARACAO,
    fmt_delta_k,
    fmt_k,
    fmt_linha_account,
    fmt_linha_type06,
)

logger = logging.getLogger(__name__)


def _esc(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# =========================================================================
#  HTML helpers
# =========================================================================

def _ranking_html(ranking: dict) -> str:
    """Monta HTML de um ranking (1 oficina) no formato relatório."""
    moeda = ranking.get("moeda", "BRL")
    simbolo = ranking.get("simbolo", "R$")
    itens = ranking.get("itens", [])

    rows: list[str] = []
    for it in itens:
        sev = it.get("severidade", "informativo")
        color = {"critico": "#e74c3c", "moderado": "#f39c12"}.get(sev, "#3498db")
        line = _esc(fmt_linha_type06(it, moeda, simbolo))
        rows.append(
            f'<tr><td style="font-weight:600;color:{color};'
            f'font-family:Consolas,monospace;padding:4px 8px;'
            f'white-space:nowrap">{line}</td></tr>'
        )
        for acc in it.get("accounts", []):
            acc_line = _esc(fmt_linha_account(acc, moeda, simbolo))
            rows.append(
                f'<tr><td style="padding-left:24px;color:#666;'
                f'font-family:Consolas,monospace;font-size:0.9em;'
                f'padding:2px 8px;white-space:nowrap">{acc_line}</td></tr>'
            )

    return "\n".join(rows)


def _ranking_consolidado_html(ranking: dict) -> str:
    """Monta HTML do ranking consolidado hierárquico."""
    moeda = ranking.get("moeda", "BRL")
    itens = ranking.get("itens", [])

    rows: list[str] = []
    for it in itens:
        sev = it.get("severidade", "informativo")
        color = {"critico": "#e74c3c", "moderado": "#f39c12"}.get(sev, "#3498db")

        # Type 05 › Type 06
        t05 = _esc(it.get("type_05", ""))
        t06 = _esc(it.get("type_06", ""))
        desvio = fmt_delta_k(it["desvio"], moeda)
        pct = it.get("desvio_pct", 0)
        rows.append(
            f'<tr><td colspan="3" style="font-weight:700;color:{color};'
            f'padding:8px 8px 2px;font-size:1em;border-bottom:1px solid #eee">'
            f'{t05} › {t06} — {_esc(desvio)} ({pct:+.1f}%)</td></tr>'
        )

        # Accounts
        for acc in it.get("accounts", []):
            acc_name = _esc(acc.get("account", ""))
            acc_desvio = fmt_delta_k(acc["desvio"], moeda)
            credit_tag = ""
            if acc.get("esperado", 0) < 0:
                credit_tag = ' <span style="color:#e67e22;font-size:0.8em">⚠️ menos receita</span>'
            rows.append(
                f'<tr><td style="padding-left:24px;font-weight:600;'
                f'color:#555;padding:3px 8px">{acc_name}</td>'
                f'<td style="color:#555;padding:3px 8px">{_esc(acc_desvio)}{credit_tag}</td>'
                f'<td></td></tr>'
            )

            # Oficinas
            for ofi in acc.get("oficinas", []):
                ofi_name = _esc(ofi.get("oficina", ""))
                ofi_dev = ofi.get("desvio", 0)
                ofi_k = f"{ofi_dev / 1000:+,.1f}k"
                rows.append(
                    f'<tr><td style="padding-left:48px;color:#777;'
                    f'font-size:0.9em;padding:1px 8px">📍 {ofi_name}</td>'
                    f'<td style="color:#777;font-size:0.9em;padding:1px 8px">'
                    f'{ofi_k}</td><td></td></tr>'
                )

                # Textos breve
                for txt in ofi.get("textos", []):
                    txt_name = _esc(txt.get("texto", ""))
                    tv = txt.get("valor", 0)
                    tv_k = f"{tv / 1000:,.1f}k"
                    rows.append(
                        f'<tr><td style="padding-left:72px;color:#999;'
                        f'font-size:0.85em;font-family:Consolas,monospace;'
                        f'padding:0 8px">• {txt_name}</td>'
                        f'<td style="color:#999;font-size:0.85em;'
                        f'padding:0 8px">{tv_k}</td><td></td></tr>'
                    )

    return "\n".join(rows)


def _tabela_html(df: pd.DataFrame) -> str:
    """Converte DataFrame da tabela de validação em HTML table estilizada."""
    if df is None or df.empty:
        return ""

    # Header
    cols = list(df.columns)
    header_cells = "".join(
        f'<th style="background:#f0f0f0;padding:6px 10px;'
        f'font-size:0.82em;white-space:nowrap;border:1px solid #ddd;'
        f'text-align:left">{_esc(c)}</th>'
        for c in cols
    )
    header = f"<tr>{header_cells}</tr>"

    # Rows
    body_rows: list[str] = []
    for _, row in df.iterrows():
        cells: list[str] = []
        for c in cols:
            val = row[c]
            style = (
                "padding:4px 10px;font-size:0.82em;white-space:nowrap;"
                "border:1px solid #eee;font-family:Consolas,monospace"
            )
            # Color delta columns
            if c in ("Real - Flex BUD P", "% Delta") and isinstance(val, (int, float)):
                if val > 0:
                    style += ";color:#e74c3c"
                elif val < 0:
                    style += ";color:#27ae60"
                formatted = f"{val:,.2f}%" if c == "% Delta" else f"{val:,.2f}"
            elif isinstance(val, (int, float)):
                formatted = f"{val:,.2f}"
            else:
                formatted = _esc(str(val))
            cells.append(f'<td style="{style}">{formatted}</td>')
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    return (
        '<table style="border-collapse:collapse;width:100%;margin-top:10px">'
        f"<thead>{header}</thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


# =========================================================================
#  Email antigo (per-oficina) — backward compat
# =========================================================================

def build_email_html(alerta: dict) -> str:
    """Monta HTML completo do e-mail para um alerta (1 oficina)."""
    meta = alerta.get("metadata", {})
    ranking = alerta.get("ranking", {})
    sev = alerta.get("severidade", "informativo")
    titulo = alerta.get("titulo", "SCI Alerta")
    moeda = meta.get("moeda", "BRL")

    sev_color = {"critico": "#e74c3c", "moderado": "#f39c12"}.get(sev, "#3498db")
    modo = meta.get("modo_label", "")
    prop = meta.get("proporcao_mes", 0)
    total_desvio = meta.get("total_desvio", 0)

    ranking_rows = _ranking_html(ranking)

    return f"""
    <html>
    <body style="font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;background:#f5f5f5">
      <div style="max-width:700px;margin:auto;background:#fff;border-radius:8px;
                  border-top:4px solid {sev_color};padding:24px">
        <h2 style="margin:0 0 4px">{_esc(titulo)}</h2>
        <p style="color:#888;margin:0 0 16px;font-size:0.85em">
          {_esc(modo)} · P = {prop:.1%} · Desvio total: {fmt_k(total_desvio, moeda)}
        </p>
        <table style="border-collapse:collapse;width:100%">
          {ranking_rows}
        </table>
        <hr style="border:none;border-top:1px solid #eee;margin:16px 0">
        <p style="font-size:0.75em;color:#aaa">
          Gerado automaticamente pelo SCI — Stellantis Cost Intelligence
        </p>
      </div>
    </body>
    </html>
    """


# =========================================================================
#  Email consolidado (com tabela de validação)
# =========================================================================

def build_email_html_consolidated(
    ranking: dict,
    modo: str,
    proporcao: float,
    tabela_df: pd.DataFrame | None = None,
) -> str:
    """Monta HTML do email consolidado: ranking hierárquico + tabela validação."""
    sev = ranking.get("severidade", "informativo")
    moeda = ranking.get("moeda", "BRL")
    periodo = ranking.get("periodo", "")
    total_desvio = ranking.get("total_desvio", 0)
    modo_label = MODOS_COMPARACAO.get(modo, modo)

    sev_color = {"critico": "#e74c3c", "moderado": "#f39c12"}.get(sev, "#3498db")

    ranking_rows = _ranking_consolidado_html(ranking)
    tabela_section = ""
    if tabela_df is not None and not tabela_df.empty:
        tabela_section = f"""
        <h3 style="margin:24px 0 8px;font-size:1em;color:#333">
          📊 Tabela de Validação
        </h3>
        {_tabela_html(tabela_df)}
        """

    return f"""
    <html>
    <body style="font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;background:#f5f5f5">
      <div style="max-width:900px;margin:auto;background:#fff;border-radius:8px;
                  border-top:4px solid {sev_color};padding:24px">
        <h2 style="margin:0 0 4px">📊 Relatório de Alertas — {_esc(periodo)}</h2>
        <p style="color:#888;margin:0 0 16px;font-size:0.85em">
          {_esc(modo_label)} · P = {proporcao:.1%} · Desvio total: {_esc(fmt_k(total_desvio, moeda))}
          · Severidade: <strong style="color:{sev_color}">{sev.upper()}</strong>
        </p>

        <h3 style="margin:0 0 8px;font-size:1em;color:#333">🔔 Ranking de Desvios</h3>
        <table style="border-collapse:collapse;width:100%">
          {ranking_rows}
        </table>

        {tabela_section}

        <hr style="border:none;border-top:1px solid #eee;margin:20px 0">
        <p style="font-size:0.75em;color:#aaa">
          Gerado automaticamente pelo SCI — Stellantis Cost Intelligence
        </p>
      </div>
    </body>
    </html>
    """


# =========================================================================
#  Envio via Graph API
# =========================================================================

def _send_graph(subject: str, html: str, email_config: dict) -> None:
    """Envia email via Microsoft Graph API."""
    from alertas.email_graph import GraphEmailClient

    client_id = email_config.get("client_id", "")
    tenant_id = email_config.get("tenant_id", "")
    if not client_id or not tenant_id:
        raise ValueError(
            "client_id e tenant_id são obrigatórios. "
            "Configure em Alertas → Notificações."
        )

    recipients = email_config.get("recipients", [])
    if not recipients:
        raise ValueError("Nenhum destinatário configurado.")

    client = GraphEmailClient(client_id=client_id, tenant_id=tenant_id)
    client.send_email(subject=subject, html_body=html, recipients=recipients)


# =========================================================================
#  Envio público
# =========================================================================

def send_alert_email(alerta: dict, email_config: dict) -> None:
    """Envia e-mail com o alerta formatado (formato antigo per-oficina)."""
    if not email_config.get("recipients"):
        logger.warning("Nenhum destinatário configurado.")
        return

    html = build_email_html(alerta)
    titulo = alerta.get("titulo", "SCI Alerta")

    _send_graph(
        subject=f"[SCI Alerta] {titulo}",
        html=html,
        email_config=email_config,
    )


def send_alert_email_consolidated(
    ranking: dict,
    email_config: dict,
    modo: str,
    proporcao: float,
    tabela_df: pd.DataFrame | None = None,
) -> None:
    """Envia email consolidado com ranking + tabela de validação."""
    if not email_config.get("recipients"):
        logger.warning("Nenhum destinatário configurado.")
        return

    html = build_email_html_consolidated(ranking, modo, proporcao, tabela_df)
    periodo = ranking.get("periodo", "")

    _send_graph(
        subject=f"[SCI Alerta] Relatório — {periodo}",
        html=html,
        email_config=email_config,
    )


def send_test_email(email_config: dict) -> None:
    """Envia email de teste para validar configuração Graph API."""
    if not email_config.get("recipients"):
        raise ValueError("Nenhum destinatário configurado.")

    html = """
    <html>
    <body style="font-family:Segoe UI,Arial,sans-serif;padding:20px">
      <div style="max-width:500px;margin:auto;background:#fff;border-radius:8px;
                  border-top:4px solid #3498db;padding:24px;text-align:center">
        <h2>✅ Email de Teste — SCI</h2>
        <p style="color:#666">Se você recebeu este email, a configuração Graph API está funcionando.</p>
        <p style="font-size:0.75em;color:#aaa">Stellantis Cost Intelligence</p>
      </div>
    </body>
    </html>
    """
    _send_graph(
        subject="[SCI] Email de Teste — Configuração OK",
        html=html,
        email_config=email_config,
    )
