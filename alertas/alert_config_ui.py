"""Página Streamlit — Configuração de Regras de Alerta.

Permite criar/editar/excluir regras com filtros por Type 05, Type 06,
Account, oficinas, moeda e modo de comparação.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import streamlit as st

from alertas.alert_engine import (
    MODOS_COMPARACAO,
    accounts_disponiveis,
    load_alert_rules,
    oficinas_disponiveis,
    save_alert_rules,
    type05_disponiveis,
    type06_disponiveis,
)
from tc_core.data.paths import listar_anos_disponiveis


# =========================================================================
#  Página principal
# =========================================================================

def render_config_page() -> None:
    st.header("⚙️ Configuração de Alertas")

    rules_data = load_alert_rules()
    rules = rules_data.get("rules", [])
    config = rules_data.get("config", {})

    tab_regras, tab_notif, tab_hist = st.tabs(
        ["📋 Regras", "📨 Notificações", "📜 Histórico"],
    )

    with tab_regras:
        _render_regras(rules_data, rules)

    with tab_notif:
        _render_notificacoes(rules_data, config)

    with tab_hist:
        _render_historico(config)


# =========================================================================
#  Tab Regras
# =========================================================================

def _render_regras(rules_data: dict, rules: list[dict]) -> None:
    if not rules:
        st.info("Nenhuma regra cadastrada.")
    else:
        for i, rule in enumerate(rules):
            _render_rule_card(rules_data, rules, rule, i)

    st.divider()
    st.subheader("➕ Nova Regra")
    _render_form_nova_regra(rules_data)


def _render_rule_card(
    rules_data: dict, rules: list[dict], rule: dict, idx: int,
) -> None:
    sev_emoji = "🟢" if rule.get("ativo") else "⚪"
    modo_label = MODOS_COMPARACAO.get(
        rule.get("modo_comparacao", ""), rule.get("modo_comparacao", ""),
    )

    with st.expander(f"{sev_emoji} {rule.get('nome', 'Regra')} — {modo_label}"):
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(f"**Ano:** {rule.get('ano', '—')}")
            ofi = rule.get("oficinas", [])
            st.markdown(f"**Oficinas:** {', '.join(ofi) if ofi else 'Todas'}")
            st.markdown(f"**Top N:** {rule.get('top_n', 10)}")
            st.markdown(f"**Moeda:** {rule.get('moeda', 'BRL')}")

            f05 = rule.get("filtro_type_05", [])
            f06 = rule.get("filtro_type_06", [])
            facc = rule.get("filtro_account", [])
            if f05:
                st.markdown(f"**Type 05:** {', '.join(f05)}")
            if f06:
                st.markdown(f"**Type 06:** {', '.join(f06)}")
            if facc:
                st.markdown(f"**Account:** {', '.join(facc)}")

        with c2:
            ativo = st.toggle(
                "Ativa", value=rule.get("ativo", True),
                key=f"cfg_toggle_{idx}",
            )
            if ativo != rule.get("ativo", True):
                rules[idx]["ativo"] = ativo
                save_alert_rules(rules_data)
                st.rerun()

            if st.button("🗑️ Excluir", key=f"cfg_del_{idx}"):
                rules.pop(idx)
                save_alert_rules(rules_data)
                st.success("Regra excluída.")
                st.rerun()


# =========================================================================
#  Form Nova Regra (com filtros cascata)
# =========================================================================

def _render_form_nova_regra(rules_data: dict) -> None:
    anos = listar_anos_disponiveis() or [2026]

    with st.form("cfg_form_nova_regra", clear_on_submit=True):
        nome = st.text_input(
            "Nome da regra",
            placeholder="Ex.: Top 5 perdas Estamparia",
        )

        fc1, fc2 = st.columns(2)
        with fc1:
            ano_sel = st.selectbox("Ano", anos, key="cfg_nr_ano")
            oficinas_list = oficinas_disponiveis(ano_sel)
            f_oficina = st.multiselect(
                "Oficina(s)",
                oficinas_list,
                help="Deixe vazio para processar TODAS as oficinas",
            )
        with fc2:
            modo = st.selectbox(
                "Modo de comparação",
                list(MODOS_COMPARACAO.keys()),
                format_func=lambda k: MODOS_COMPARACAO[k],
            )
            top_n = st.number_input("Top N (piores Type 06)", 1, 50, 10)

        # --- Moeda ---
        moeda = st.selectbox("Moeda", ["BRL", "EUR", "USD"], index=0)

        # --- Filtros dimensionais (cascata) ---
        st.markdown("**Filtros dimensionais** *(deixe vazio = todos)*")
        fd1, fd2, fd3 = st.columns(3)

        with fd1:
            t05_opts = type05_disponiveis(ano_sel)
            f_type05 = st.multiselect("Type 05", t05_opts)

        with fd2:
            t06_opts = type06_disponiveis(ano_sel, f_type05 or None)
            f_type06 = st.multiselect("Type 06", t06_opts)

        with fd3:
            acc_opts = accounts_disponiveis(ano_sel, f_type06 or None)
            f_account = st.multiselect("Account", acc_opts)

        submitted = st.form_submit_button("💾 Salvar Regra")
        if submitted:
            if not nome.strip():
                st.error("Informe o nome da regra.")
            else:
                nova_regra = {
                    "id": str(uuid.uuid4()),
                    "nome": nome.strip(),
                    "ativo": True,
                    "criado_em": datetime.now().isoformat(timespec="seconds"),
                    "ano": ano_sel,
                    "oficinas": f_oficina,
                    "modo_comparacao": modo,
                    "top_n": int(top_n),
                    "moeda": moeda,
                    "filtro_type_05": f_type05,
                    "filtro_type_06": f_type06,
                    "filtro_account": f_account,
                }
                rules_data["rules"].append(nova_regra)
                save_alert_rules(rules_data)
                st.success(f"Regra **{nome}** criada!")
                st.rerun()


# =========================================================================
#  Helpers — Graph Auth UI
# =========================================================================

def _render_graph_auth_status(email_cfg: dict) -> None:
    """Exibe status de autenticação Graph API."""
    client_id = email_cfg.get("client_id", "")
    tenant_id = email_cfg.get("tenant_id", "")
    if not client_id or not tenant_id:
        st.info("🔧 Configure Client ID e Tenant ID para habilitar e-mail via Graph API.")
        return

    try:
        from alertas.email_graph import GraphEmailClient
        client = GraphEmailClient(client_id=client_id, tenant_id=tenant_id)
        if client.is_authenticated():
            account = client.get_account_info()
            username = account.get("username", "desconhecido") if account else "desconhecido"
            st.success(f"✅ Autenticado como **{username}** (token será renovado automaticamente)")
        else:
            st.warning("⚠️ Não autenticado — clique em **🔑 Autenticar com Microsoft** abaixo.")
    except Exception as e:
        st.warning(f"⚠️ Erro ao verificar autenticação: {e}")


def _render_graph_auth_button(config: dict) -> None:
    """Botão de autenticação Device Code Flow."""
    st.subheader("🔑 Autenticação Microsoft Graph")
    email_cfg = config.get("email", {})
    client_id = email_cfg.get("client_id", "")
    tenant_id = email_cfg.get("tenant_id", "")

    c_auth, c_logout = st.columns(2)

    with c_auth:
        if st.button("🔑 Autenticar com Microsoft"):
            if not client_id or not tenant_id:
                st.error("Configure Client ID e Tenant ID primeiro e salve.")
                return
            try:
                from alertas.email_graph import GraphEmailClient
                client = GraphEmailClient(client_id=client_id, tenant_id=tenant_id)
                flow = client.authenticate()

                user_code = flow.get("user_code", "???")
                verification_uri = flow.get("verification_uri", "https://microsoft.com/devicelogin")

                st.info(
                    f"📋 Abra: **{verification_uri}**\n\n"
                    f"🔑 Insira o código: **{user_code}**\n\n"
                    f"⏳ Aguardando autenticação no browser..."
                )

                with st.spinner("Aguardando login no browser..."):
                    result = client.acquire_token_by_device_flow(flow)

                account = result.get("id_token_claims", {}).get(
                    "preferred_username", "autenticado",
                )
                st.success(f"✅ Autenticado como **{account}**!")
                st.rerun()

            except Exception as e:
                st.error(f"❌ Falha na autenticação: {e}")

    with c_logout:
        if st.button("🚪 Desconectar"):
            if not client_id or not tenant_id:
                st.info("Nada para desconectar.")
                return
            try:
                from alertas.email_graph import GraphEmailClient
                client = GraphEmailClient(client_id=client_id, tenant_id=tenant_id)
                client.logout()
                st.success("Cache de token removido.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")


# =========================================================================
#  Tab Notificações
# =========================================================================

def _render_notificacoes(rules_data: dict, config: dict) -> None:
    st.subheader("Canais de notificação")

    notif = config.get("notifications_enabled", {})

    # --- Status de autenticação Graph ---
    email_cfg = config.get("email", {})
    _render_graph_auth_status(email_cfg)

    with st.form("cfg_notif_form"):
        n_int = st.checkbox("Interna (app)", value=notif.get("internal", True))
        n_email = st.checkbox("E-mail", value=notif.get("email", False))
        n_teams = st.checkbox("Teams", value=notif.get("teams", False))

        st.divider()
        st.markdown("**E-mail (Microsoft Graph API)**")
        client_id = st.text_input(
            "Application (Client) ID",
            value=email_cfg.get("client_id", ""),
            help="ID do App Registration no Azure AD",
        )
        tenant_id = st.text_input(
            "Directory (Tenant) ID",
            value=email_cfg.get("tenant_id", "d852d5cd-724c-4128-8812-ffa5db3f8507"),
            help="ID do tenant Azure AD da Stellantis",
        )
        sender = st.text_input(
            "Remetente (informativo)",
            value=email_cfg.get("sender", ""),
            help="A conta que enviará é a autenticada. Apenas para referência.",
        )
        recipients = st.text_area(
            "Destinatários (1 por linha)",
            value="\n".join(email_cfg.get("recipients", [])),
        )

        st.divider()
        st.markdown("**Teams (Webhook)**")
        webhook = st.text_input(
            "URL Webhook", value=config.get("teams_webhook_url", ""),
        )

        st.divider()
        st.markdown("**⏰ Agendamento Automático Diário**")
        sched_cfg = config.get("schedule", {})
        sched_enabled = st.checkbox(
            "Ativar envio diário automático",
            value=sched_cfg.get("enabled", False),
        )
        sched_hour = st.selectbox(
            "Horário de envio",
            options=[f"{h:02d}:00" for h in range(6, 22)],
            index=max(0, min(15, sched_cfg.get("hour", 8) - 6)),
        )

        if st.form_submit_button("💾 Salvar Notificações"):
            config["notifications_enabled"] = {
                "internal": n_int,
                "email": n_email,
                "teams": n_teams,
            }
            config["email"] = {
                "provider": "graph",
                "client_id": client_id.strip(),
                "tenant_id": tenant_id.strip(),
                "sender": sender.strip(),
                "recipients": [
                    r.strip() for r in recipients.split("\n") if r.strip()
                ],
            }
            config["teams_webhook_url"] = webhook
            config["schedule"] = {
                "enabled": sched_enabled,
                "hour": int(sched_hour.split(":")[0]),
            }
            rules_data["config"] = config
            save_alert_rules(rules_data)
            st.success("Configuração salva!")

            # Reiniciar scheduler se necessário
            from alertas.scheduler import restart_scheduler
            restart_scheduler()
            if sched_enabled:
                st.info(f"⏰ Agendamento ativo — envio diário às {sched_hour}.")

    # --- Botões de autenticação e teste (fora do form) ---
    st.divider()
    _render_graph_auth_button(config)

    st.divider()
    st.subheader("🧪 Testar Envio")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("📧 Enviar email de teste"):
            email_cfg = config.get("email", {})
            if not email_cfg.get("recipients"):
                st.error("Configure destinatários primeiro.")
            elif not email_cfg.get("client_id"):
                st.error("Configure o Client ID primeiro.")
            else:
                from alertas.notifications_email import send_test_email
                try:
                    send_test_email(email_cfg)
                    st.success("✅ Email de teste enviado com sucesso!")
                except Exception as e:
                    st.error(f"❌ Falha ao enviar: {e}")
    with c2:
        if st.button("📤 Enviar teste Teams"):
            webhook = config.get("teams_webhook_url", "")
            if not webhook:
                st.error("Configure o URL do Webhook primeiro.")
            else:
                import json
                import urllib.request
                test_card = {
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "themeColor": "3498DB",
                    "summary": "SCI — Teste",
                    "sections": [{
                        "activityTitle": "✅ Teste de Conexão — SCI",
                        "activitySubtitle": "Se você vê esta mensagem, o webhook está funcionando.",
                        "markdown": True,
                    }],
                }
                try:
                    payload = json.dumps(test_card).encode("utf-8")
                    req = urllib.request.Request(
                        webhook, data=payload,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        resp.read()
                    st.success("✅ Mensagem de teste enviada ao Teams!")
                except Exception as e:
                    st.error(f"❌ Falha ao enviar: {e}")


# =========================================================================
#  Tab Histórico
# =========================================================================

def _render_historico(config: dict) -> None:
    ultima = config.get("ultima_execucao")
    if ultima:
        st.info(f"Última execução automática: **{ultima}**")
    else:
        st.info("Nenhuma execução automática registrada.")


# =========================================================================
#  Execução (Streamlit st.Page)
# =========================================================================

render_config_page()
