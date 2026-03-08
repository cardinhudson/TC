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
    normalizar_filtros_dependentes,
    normalizar_regra_alerta,
    oficinas_disponiveis,
    save_alert_rules,
    type05_disponiveis,
    type06_disponiveis,
)
from tc_core.data.paths import listar_anos_disponiveis


# =========================================================================
#  Página principal
# =========================================================================

_RULE_FIELD_KEYS = {
    "id": "cfg_rule_edit_id",
    "nome": "cfg_rule_nome",
    "ano": "cfg_rule_ano",
    "oficinas": "cfg_rule_oficinas",
    "modo": "cfg_rule_modo",
    "top_n": "cfg_rule_top_n",
    "moeda": "cfg_rule_moeda",
    "type05": "cfg_rule_type05",
    "type06": "cfg_rule_type06",
    "account": "cfg_rule_account",
    "sched_enabled": "cfg_rule_sched_enabled",
    "sched_freq": "cfg_rule_sched_freq",
    "sched_hour": "cfg_rule_sched_hour",
    "sched_minute": "cfg_rule_sched_minute",
    "sched_start_day": "cfg_rule_sched_start_day",
    "sched_weekdays": "cfg_rule_sched_weekdays",
    "sched_monthdays": "cfg_rule_sched_monthdays",
}

_WEEKDAY_OPTIONS = [
    "Segunda",
    "Terca",
    "Quarta",
    "Quinta",
    "Sexta",
    "Sabado",
    "Domingo",
]

_FREQUENCY_LABELS = {
    "daily": "Diario",
    "weekly": "Semanal",
    "monthly": "Mensal",
}


def _defaults_regra_ui(rules_data: dict, ano_padrao: int) -> dict:
    legacy_schedule = rules_data.get("config", {}).get("schedule", {})
    return {
        "id": None,
        "nome": "",
        "ano": ano_padrao,
        "oficinas": [],
        "modo": list(MODOS_COMPARACAO.keys())[0],
        "top_n": 10,
        "moeda": "BRL",
        "type05": [],
        "type06": [],
        "account": [],
        "sched_enabled": bool(legacy_schedule.get("enabled", False)),
        "sched_freq": "daily",
        "sched_hour": int(legacy_schedule.get("hour", 8)),
        "sched_minute": 0,
        "sched_start_day": 1,
        "sched_weekdays": [],
        "sched_monthdays": [],
    }


def _popular_estado_regra(rules_data: dict, anos: list[int], rule: dict | None = None) -> None:
    ano_padrao = anos[0] if anos else 2026
    values = _defaults_regra_ui(rules_data, ano_padrao)
    if rule:
        rule = normalizar_regra_alerta(rule, rules_data.get("config", {}))
        schedule = rule.get("schedule", {})
        values.update({
            "id": rule.get("id"),
            "nome": rule.get("nome", ""),
            "ano": int(rule.get("ano", ano_padrao)),
            "oficinas": list(rule.get("oficinas", [])),
            "modo": rule.get("modo_comparacao", values["modo"]),
            "top_n": int(rule.get("top_n", 10)),
            "moeda": rule.get("moeda", "BRL"),
            "type05": list(rule.get("filtro_type_05", [])),
            "type06": list(rule.get("filtro_type_06", [])),
            "account": list(rule.get("filtro_account", [])),
            "sched_enabled": bool(schedule.get("enabled", False)),
            "sched_freq": schedule.get("frequency", "daily"),
            "sched_hour": int(schedule.get("hour", values["sched_hour"])),
            "sched_minute": int(schedule.get("minute", 0)),
            "sched_start_day": int(schedule.get("start_day_of_month", 1)),
            "sched_weekdays": list(schedule.get("days_of_week", [])),
            "sched_monthdays": list(schedule.get("days_of_month", [])),
        })

    for field, key in _RULE_FIELD_KEYS.items():
        st.session_state[key] = values[field]


def _garantir_estado_regra(rules_data: dict, anos: list[int]) -> None:
    if _RULE_FIELD_KEYS["nome"] not in st.session_state:
        _popular_estado_regra(rules_data, anos)


def _limpar_estado_regra(rules_data: dict, anos: list[int]) -> None:
    _popular_estado_regra(rules_data, anos)


def _resumo_agendamento(rule: dict) -> str:
    schedule = rule.get("schedule", {})
    if not schedule.get("enabled", False):
        return "Manual"

    horario = f"{int(schedule.get('hour', 8)):02d}:{int(schedule.get('minute', 0)):02d}"
    freq = schedule.get("frequency", "daily")
    if freq == "weekly":
        dias = ", ".join(schedule.get("days_of_week", [])) or "sem dias"
        return f"Semanal ({dias}) as {horario}"
    if freq == "monthly":
        dias = ", ".join(str(v) for v in schedule.get("days_of_month", [])) or "sem dias"
        return f"Mensal (dias {dias}) as {horario}"
    inicio = int(schedule.get("start_day_of_month", 1))
    if inicio > 1:
        return f"Diario a partir do dia {inicio} as {horario}"
    return f"Diario as {horario}"

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
    anos = listar_anos_disponiveis() or [2026]
    _garantir_estado_regra(rules_data, anos)

    if not rules:
        st.info("Nenhuma regra cadastrada.")
    else:
        for i, rule in enumerate(rules):
            _render_rule_card(rules_data, rules, rule, i, anos)

    st.divider()
    editando = bool(st.session_state.get(_RULE_FIELD_KEYS["id"]))
    st.subheader("✏️ Editar Regra" if editando else "➕ Nova Regra")
    _render_form_nova_regra(rules_data, anos)


def _render_rule_card(
    rules_data: dict, rules: list[dict], rule: dict, idx: int, anos: list[int],
) -> None:
    rule = normalizar_regra_alerta(rule, rules_data.get("config", {}))
    rules[idx] = rule
    sev_emoji = "🟢" if rule.get("ativo") else "⚪"
    modo_label = MODOS_COMPARACAO.get(
        rule.get("modo_comparacao", ""), rule.get("modo_comparacao", ""),
    )

    with st.expander(f"{sev_emoji} {rule.get('nome', 'Regra')} — {modo_label}"):
        c1, c2 = st.columns([3, 1.2])
        with c1:
            st.markdown(f"**Ano:** {rule.get('ano', '—')}")
            ofi = rule.get("oficinas", [])
            st.markdown(f"**Oficinas:** {', '.join(ofi) if ofi else 'Todas'}")
            st.markdown(f"**Top N:** {rule.get('top_n', 10)}")
            st.markdown(f"**Moeda:** {rule.get('moeda', 'BRL')}")
            st.markdown(f"**Agendamento:** {_resumo_agendamento(rule)}")

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

            if st.button("✏️ Editar", key=f"cfg_edit_{idx}"):
                _popular_estado_regra(rules_data, anos, rules[idx])
                st.rerun()

            if st.button("🗑️ Excluir", key=f"cfg_del_{idx}"):
                edit_id = st.session_state.get(_RULE_FIELD_KEYS["id"])
                rules.pop(idx)
                save_alert_rules(rules_data)
                if edit_id == rule.get("id"):
                    _limpar_estado_regra(rules_data, anos)
                st.success("Regra excluída.")
                st.rerun()


# =========================================================================
#  Form Nova Regra (com filtros cascata)
# =========================================================================

def _render_form_nova_regra(rules_data: dict, anos: list[int]) -> None:
    edit_id = st.session_state.get(_RULE_FIELD_KEYS["id"])
    ano_key = _RULE_FIELD_KEYS["ano"]
    type05_key = _RULE_FIELD_KEYS["type05"]
    type06_key = _RULE_FIELD_KEYS["type06"]
    account_key = _RULE_FIELD_KEYS["account"]

    nome = st.text_input(
        "Nome da regra",
        key=_RULE_FIELD_KEYS["nome"],
        placeholder="Ex.: Top 5 perdas Estamparia",
    )

    fc1, fc2 = st.columns(2)
    with fc1:
        st.selectbox("Ano", anos, key=ano_key)
        oficinas_list = oficinas_disponiveis(st.session_state[ano_key])
        oficinas_validas = [v for v in st.session_state[_RULE_FIELD_KEYS["oficinas"]] if v in oficinas_list]
        if oficinas_validas != st.session_state[_RULE_FIELD_KEYS["oficinas"]]:
            st.session_state[_RULE_FIELD_KEYS["oficinas"]] = oficinas_validas
        st.multiselect(
            "Oficina(s)",
            oficinas_list,
            key=_RULE_FIELD_KEYS["oficinas"],
            help="Deixe vazio para processar TODAS as oficinas",
        )
    with fc2:
        st.selectbox(
            "Modo de comparação",
            list(MODOS_COMPARACAO.keys()),
            key=_RULE_FIELD_KEYS["modo"],
            format_func=lambda k: MODOS_COMPARACAO[k],
        )
        st.number_input("Top N (piores Type 06)", 1, 50, key=_RULE_FIELD_KEYS["top_n"])

    st.selectbox("Moeda", ["BRL", "EUR", "USD"], key=_RULE_FIELD_KEYS["moeda"])

    ano_sel = int(st.session_state[ano_key])
    filtros = normalizar_filtros_dependentes(
        ano_sel,
        st.session_state[type05_key],
        st.session_state[type06_key],
        st.session_state[account_key],
    )
    st.session_state[type05_key] = filtros["filtro_type_05"]
    st.session_state[type06_key] = filtros["filtro_type_06"]
    st.session_state[account_key] = filtros["filtro_account"]

    st.markdown("**Filtros dimensionais** *(deixe vazio = todos)*")
    fd1, fd2, fd3 = st.columns(3)
    with fd1:
        st.multiselect("Type 05", type05_disponiveis(ano_sel), key=type05_key)
    with fd2:
        st.multiselect(
            "Type 06",
            type06_disponiveis(ano_sel, st.session_state[type05_key] or None),
            key=type06_key,
        )
    with fd3:
        st.multiselect(
            "Account",
            accounts_disponiveis(
                ano_sel,
                st.session_state[type06_key] or None,
            ),
            key=account_key,
        )

    st.markdown("**Agendamento por regra**")
    ag1, ag2, ag3 = st.columns(3)
    with ag1:
        st.checkbox("Ativar envio automatico", key=_RULE_FIELD_KEYS["sched_enabled"])
        st.selectbox(
            "Periodicidade",
            list(_FREQUENCY_LABELS.keys()),
            key=_RULE_FIELD_KEYS["sched_freq"],
            format_func=lambda k: _FREQUENCY_LABELS[k],
        )
    with ag2:
        st.selectbox("Hora", list(range(24)), key=_RULE_FIELD_KEYS["sched_hour"], format_func=lambda v: f"{v:02d}")
        st.selectbox("Minuto", [0, 15, 30, 45], key=_RULE_FIELD_KEYS["sched_minute"], format_func=lambda v: f"{v:02d}")
    with ag3:
        if st.session_state[_RULE_FIELD_KEYS["sched_freq"]] == "daily":
            st.number_input(
                "Comecar no dia do mes",
                min_value=1,
                max_value=31,
                key=_RULE_FIELD_KEYS["sched_start_day"],
                help="Antes deste dia a regra nao envia notificacoes automaticas.",
            )
        elif st.session_state[_RULE_FIELD_KEYS["sched_freq"]] == "weekly":
            st.multiselect(
                "Dias da semana",
                _WEEKDAY_OPTIONS,
                key=_RULE_FIELD_KEYS["sched_weekdays"],
            )
        else:
            st.multiselect(
                "Dias do mes",
                list(range(1, 32)),
                key=_RULE_FIELD_KEYS["sched_monthdays"],
            )

    bc1, bc2 = st.columns([1, 1])
    salvar_label = "💾 Atualizar Regra" if edit_id else "💾 Salvar Regra"
    with bc1:
        salvar = st.button(salvar_label, type="primary")
    with bc2:
        cancelar = st.button("Cancelar edicao" if edit_id else "Limpar")

    if cancelar:
        _limpar_estado_regra(rules_data, anos)
        st.rerun()

    if salvar:
        if not nome.strip():
            st.error("Informe o nome da regra.")
            return

        schedule = {
            "enabled": bool(st.session_state[_RULE_FIELD_KEYS["sched_enabled"]]),
            "frequency": st.session_state[_RULE_FIELD_KEYS["sched_freq"]],
            "hour": int(st.session_state[_RULE_FIELD_KEYS["sched_hour"]]),
            "minute": int(st.session_state[_RULE_FIELD_KEYS["sched_minute"]]),
            "start_day_of_month": int(st.session_state[_RULE_FIELD_KEYS["sched_start_day"]]),
            "days_of_week": list(st.session_state[_RULE_FIELD_KEYS["sched_weekdays"]]),
            "days_of_month": [int(v) for v in st.session_state[_RULE_FIELD_KEYS["sched_monthdays"]]],
        }

        regra = normalizar_regra_alerta({
            "id": edit_id or str(uuid.uuid4()),
            "nome": nome.strip(),
            "ativo": True,
            "criado_em": datetime.now().isoformat(timespec="seconds"),
            "ano": ano_sel,
            "oficinas": list(st.session_state[_RULE_FIELD_KEYS["oficinas"]]),
            "modo_comparacao": st.session_state[_RULE_FIELD_KEYS["modo"]],
            "top_n": int(st.session_state[_RULE_FIELD_KEYS["top_n"]]),
            "moeda": st.session_state[_RULE_FIELD_KEYS["moeda"]],
            "filtro_type_05": list(st.session_state[type05_key]),
            "filtro_type_06": list(st.session_state[type06_key]),
            "filtro_account": list(st.session_state[account_key]),
            "schedule": schedule,
        }, rules_data.get("config", {}))

        if edit_id:
            for idx, existing in enumerate(rules_data["rules"]):
                if existing.get("id") == edit_id:
                    regra["ativo"] = existing.get("ativo", True)
                    regra["criado_em"] = existing.get("criado_em", regra["criado_em"])
                    rules_data["rules"][idx] = regra
                    break
            mensagem = f"Regra **{regra['nome']}** atualizada!"
        else:
            rules_data["rules"].append(regra)
            mensagem = f"Regra **{regra['nome']}** criada!"

        save_alert_rules(rules_data)
        _limpar_estado_regra(rules_data, anos)
        st.success(mensagem)
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

        st.info(
            "Os alertas automáticos são disparados ao final do processamento de dados. "
            "Use a Central de Alertas para disparo manual quando necessário."
        )

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
            rules_data["config"] = config
            save_alert_rules(rules_data)
            st.success("Configuração salva!")

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
