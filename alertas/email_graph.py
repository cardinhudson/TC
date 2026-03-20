"""Envio de e-mail via Microsoft Graph API com autenticação OAuth 2.0.

Substitui o envio SMTP por Microsoft Graph API + Device Code Flow (MSAL).
O token é persistido em cache local e renovado automaticamente.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path

import msal
import requests

logger = logging.getLogger(__name__)

_TOKEN_CACHE_PATH = Path(__file__).parent / ".token_cache.json"
_GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"
_SCOPES = ["Mail.Send", "User.Read", "offline_access"]

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2  # seconds


# =========================================================================
#  Exceções
# =========================================================================

class GraphAuthError(Exception):
    """Falha de autenticação (token expirado, sem refresh)."""


class GraphPermissionError(Exception):
    """Permissão insuficiente (403)."""


class GraphSendError(Exception):
    """Falha genérica de envio (rede, rate limit, etc.)."""


# =========================================================================
#  Helpers
# =========================================================================

def parse_email(addr: str) -> str:
    """Extrai bare email de formatos como '"Nome" <email@x.com>'."""
    m = re.search(r"<([^>]+)>", addr)
    if m:
        return m.group(1).strip()
    return addr.strip()


def build_send_mail_payload(
    subject: str,
    html_body: str,
    recipients: list[str],
) -> dict:
    """Monta o JSON payload para POST /me/sendMail."""
    to_list = []
    for r in recipients:
        bare = parse_email(r)
        to_list.append({"emailAddress": {"address": bare}})

    return {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body,
            },
            "toRecipients": to_list,
        },
        "saveToSentItems": True,
    }


# =========================================================================
#  Cliente Graph
# =========================================================================

class GraphEmailClient:
    """Cliente de e-mail via Microsoft Graph API com Device Code Flow.

    Em ambiente cloud (Databricks), o Device Code Flow não está disponível
    (sem browser interativo). Use Teams webhook como alternativa.
    """

    def __init__(
        self,
        client_id: str,
        tenant_id: str,
        token_cache_path: str | Path | None = None,
    ):
        from tc_core.utils.portabilidade import is_cloud

        self._client_id = client_id
        self._tenant_id = tenant_id
        self._is_cloud = is_cloud()
        if self._is_cloud:
            self._cache_path = None
            logger.warning(
                "GraphEmailClient em cloud — Device Code Flow indisponível. "
                "Use Teams webhook para notificações."
            )
        else:
            self._cache_path = Path(token_cache_path or _TOKEN_CACHE_PATH)
        self._cache = self._load_cache()
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._app = msal.PublicClientApplication(
            client_id,
            authority=authority,
            token_cache=self._cache,
        )

    # ----- Cache -----

    def _load_cache(self) -> msal.SerializableTokenCache:
        cache = msal.SerializableTokenCache()
        if self._cache_path and self._cache_path.exists():
            try:
                cache.deserialize(self._cache_path.read_text(encoding="utf-8"))
            except Exception:
                logger.warning("Cache de token corrompido — será recriado.")
        return cache

    def _save_cache(self) -> None:
        if self._cache_path and self._cache.has_state_changed:
            self._cache_path.write_text(
                self._cache.serialize(), encoding="utf-8",
            )

    # ----- Auth -----

    def get_accounts(self) -> list[dict]:
        return self._app.get_accounts()

    def is_authenticated(self) -> bool:
        return len(self.get_accounts()) > 0

    def get_account_info(self) -> dict | None:
        accounts = self.get_accounts()
        return accounts[0] if accounts else None

    def authenticate(self) -> dict:
        """Inicia Device Code Flow. Retorna dict com 'user_code' e 'message'."""
        flow = self._app.initiate_device_flow(scopes=_SCOPES)
        if "user_code" not in flow:
            raise GraphAuthError(
                f"Falha ao iniciar Device Code Flow: {flow.get('error_description', flow)}"
            )
        return flow

    def acquire_token_by_device_flow(self, flow: dict) -> dict:
        """Bloqueia até o usuário completar o login no browser."""
        result = self._app.acquire_token_by_device_flow(flow)
        self._save_cache()
        if "access_token" not in result:
            raise GraphAuthError(
                result.get("error_description", "Falha na autenticação.")
            )
        return result

    def acquire_token_silent(self) -> str | None:
        """Tenta obter access_token silenciosamente (refresh automático)."""
        accounts = self.get_accounts()
        if not accounts:
            return None
        result = self._app.acquire_token_silent(
            scopes=_SCOPES, account=accounts[0],
        )
        self._save_cache()
        if result and "access_token" in result:
            return result["access_token"]
        return None

    def _get_token(self) -> str:
        """Obtém token válido ou levanta GraphAuthError."""
        token = self.acquire_token_silent()
        if token:
            return token
        raise GraphAuthError(
            "Token expirado e não foi possível renovar. "
            "Re-autentique via Configuração → Notificações."
        )

    # ----- Envio -----

    def send_email(
        self,
        subject: str,
        html_body: str,
        recipients: list[str],
    ) -> None:
        """Envia email via Graph API POST /me/sendMail."""
        token = self._get_token()
        payload = build_send_mail_payload(subject, html_body, recipients)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            resp = requests.post(
                f"{_GRAPH_ENDPOINT}/me/sendMail",
                headers=headers,
                json=payload,
                timeout=30,
            )

            if resp.status_code in (200, 202):
                logger.info("Email enviado via Graph API.")
                return

            if resp.status_code == 401:
                raise GraphAuthError(f"401 — Token inválido: {resp.text}")
            if resp.status_code == 403:
                raise GraphPermissionError(
                    f"403 — Permissão insuficiente (Mail.Send): {resp.text}"
                )

            if resp.status_code in (429, 503):
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "Graph API %s — retry %d/%d em %ds",
                    resp.status_code, attempt + 1, _MAX_RETRIES, delay,
                )
                last_exc = GraphSendError(f"{resp.status_code}: {resp.text}")
                time.sleep(delay)
                continue

            raise GraphSendError(f"{resp.status_code}: {resp.text}")

        if last_exc:
            raise last_exc

    # ----- Logout -----

    def logout(self) -> None:
        """Remove cache de token local."""
        if self._cache_path.exists():
            self._cache_path.unlink()
            logger.info("Cache de token removido.")
