"""
TC Copilot — Integração com OpenAI.

Cliente simples para gerar textos analíticos e responder consultas.
Com fallback automático se a chave não estiver configurada.
"""

from __future__ import annotations

import logging

from tc_copilot.config import carregar_api_key, carregar_modelo
from tc_copilot.prompts import SYSTEM_PROMPTS, PROMPTS

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CLIENTE OPENAI
# ═══════════════════════════════════════════════════════════════

def _criar_cliente(api_key: str | None = None):
    """Cria e retorna um cliente OpenAI. Retorna None se indisponível."""
    key = api_key or carregar_api_key()
    if not key:
        return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=key)
    except Exception as e:
        logger.warning("Erro ao criar cliente OpenAI: %s", e)
        return None


def gerar_texto(
    prompt: str,
    system_prompt: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 2000,
) -> str | None:
    """
    Gera texto usando a API da OpenAI.

    Returns:
        Texto gerado ou None se falhar.
    """
    client = _criar_cliente(api_key)
    if client is None:
        return None

    model = model or carregar_modelo()
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error("Erro ao chamar OpenAI: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════
#  GERAÇÃO DE SEÇÕES DO RELATÓRIO
# ═══════════════════════════════════════════════════════════════

def gerar_secao_relatorio(
    tipo_secao: str,
    dados_formatados: str,
    mes: str,
    ano: int,
    idioma: str = "pt-BR",
    api_key: str | None = None,
    model: str | None = None,
    ano_anterior: int | None = None,
    oficina: str | None = None,
) -> str:
    """
    Gera texto para uma seção do relatório mensal.

    Args:
        tipo_secao: Chave do prompt (ex: 'analise_volume', 'comparativos', 'oficina')
        dados_formatados: Texto com dados numéricos para a LLM analisar
        mes: Nome do mês
        ano: Ano
        idioma: 'pt-BR' ou 'en'
        api_key: Chave OpenAI (opcional, usa config se não fornecida)
        model: Modelo LLM (opcional)
        ano_anterior: Ano anterior (para comparativos YoY)
        oficina: Nome da oficina (para seções de oficina)

    Returns:
        Texto gerado pela LLM ou texto fallback se LLM indisponível.
    """
    if tipo_secao not in PROMPTS:
        return f"[Seção '{tipo_secao}' não definida]"

    template = PROMPTS[tipo_secao].get(idioma, PROMPTS[tipo_secao]["pt-BR"])
    system = SYSTEM_PROMPTS.get(idioma, SYSTEM_PROMPTS["pt-BR"])

    # Formatar prompt com dados
    fmt_vars = {
        "dados": dados_formatados,
        "mes": mes,
        "ano": ano,
        "ano_anterior": ano_anterior or (ano - 1),
    }
    if oficina:
        fmt_vars["oficina"] = oficina
    prompt = template.format(**fmt_vars)

    # Tentar gerar com LLM
    texto = gerar_texto(prompt, system_prompt=system, api_key=api_key, model=model)

    if texto:
        return texto

    # Fallback: retornar dados brutos como texto
    return _fallback_texto(tipo_secao, dados_formatados, mes, ano, idioma)


def responder_consulta(
    pergunta: str,
    contexto_pdf: str,
    idioma: str = "pt-BR",
    api_key: str | None = None,
    model: str | None = None,
) -> str:
    """
    Responde uma pergunta sobre o conteúdo do PDF anual.
    DEPRECATED — use responder_consulta_live() para consultas ao vivo.
    """
    template = PROMPTS["consulta_pdf"].get(idioma, PROMPTS["consulta_pdf"]["pt-BR"])
    system = SYSTEM_PROMPTS.get(idioma, SYSTEM_PROMPTS["pt-BR"])

    prompt = template.format(contexto=contexto_pdf, pergunta=pergunta)
    texto = gerar_texto(prompt, system_prompt=system, api_key=api_key, model=model)

    if texto:
        return texto

    if idioma == "pt-BR":
        return (
            "⚠️ Não foi possível consultar a LLM. "
            "Verifique se a chave da OpenAI está configurada corretamente."
        )
    return (
        "⚠️ Could not query the LLM. "
        "Please check if the OpenAI key is configured correctly."
    )


def responder_consulta_live(
    pergunta: str,
    contexto_dados: str,
    historico_chat: list[dict] | None = None,
    idioma: str = "pt-BR",
    api_key: str | None = None,
    model: str | None = None,
    moeda: str = "EUR",
) -> str:
    """
    Responde uma pergunta usando dados ao vivo dos parquets (não PDF).
    Inclui documentação do sistema como contexto adicional.

    Args:
        pergunta: Pergunta do usuário
        contexto_dados: Texto com dados formatados dos parquets
        historico_chat: Lista de msgs anteriores [{"role": "user/assistant", "content": "..."}]
        idioma: 'pt-BR' ou 'en'
        api_key: Chave OpenAI
        model: Modelo LLM
        moeda: Moeda ativa (BRL, USD, EUR) para formatar o system prompt

    Returns:
        Resposta da LLM.
    """
    template = PROMPTS.get("consulta_live", PROMPTS["consulta_pdf"]).get(
        idioma, PROMPTS.get("consulta_live", PROMPTS["consulta_pdf"])["pt-BR"]
    )
    system = SYSTEM_PROMPTS.get(idioma, SYSTEM_PROMPTS["pt-BR"])

    # Formatar system prompt com moeda dinâmica
    system = system.format(moeda=moeda)

    # Injetar dicionário semântico de colunas
    from tc_copilot.prompts import DICIONARIO_COLUNAS
    dicionario = DICIONARIO_COLUNAS.get(idioma, DICIONARIO_COLUNAS["pt-BR"])
    system = system + "\n\n" + dicionario

    # Incluir documentação do sistema no system prompt (SOMENTE chatbot)
    try:
        from tc_copilot.data_collector import carregar_documentacao_sistema
        doc_text = carregar_documentacao_sistema()
        if doc_text:
            # Limitar a ~30K chars para não estourar contexto
            if len(doc_text) > 30000:
                doc_text = doc_text[:30000] + "\n\n[... documentação truncada ...]"
            system = (
                system + "\n\n"
                "--- DOCUMENTAÇÃO DO SISTEMA ---\n"
                "Use esta documentação para responder perguntas sobre regras de cálculo, "
                "arquitetura, pipeline de dados e funcionalidades do sistema.\n\n"
                + doc_text
            )
    except Exception:
        pass  # Se falhar, segue sem documentação

    prompt = template.format(contexto=contexto_dados, pergunta=pergunta, moeda=moeda)

    # Montar mensagens com histórico
    messages = [{"role": "system", "content": system}]
    if historico_chat:
        # Incluir últimas 10 mensagens de contexto
        for msg in historico_chat[-10:]:
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": prompt})

    # Chamar API com mensagens completas
    _api_key = api_key or carregar_api_key()
    _model = model or carregar_modelo()

    client = _criar_cliente(_api_key)
    if not client:
        if idioma == "pt-BR":
            return "⚠️ Chave OpenAI não configurada. Configure na aba ⚙️ Config."
        return "⚠️ OpenAI key not configured. Set it in the ⚙️ Config tab."

    try:
        response = client.chat.completions.create(
            model=_model,
            messages=messages,
            temperature=0.3,
            max_tokens=2000,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error("Erro na consulta live: %s", e)
        if idioma == "pt-BR":
            return f"⚠️ Erro ao consultar LLM: {e}"
        return f"⚠️ Error querying LLM: {e}"


# ═══════════════════════════════════════════════════════════════
#  FALLBACK (sem LLM)
# ═══════════════════════════════════════════════════════════════

_FALLBACK_HEADERS = {
    "pt-BR": {
        "analise_volume": "Dados de volume para análise:",
        "variacoes_modelo": "Dados de variações por modelo:",
        "comparativos": "Dados comparativos (Real vs Flex, Mês Anterior, Budget, Ano Anterior):",
        "real_vs_mes_anterior": "Dados comparativos Real vs Mês Anterior:",
        "real_vs_flex": "Dados comparativos Real vs Flex Budget:",
        "real_vs_budget": "Dados comparativos Real vs Budget:",
        "real_vs_ano_anterior": "Dados comparativos Real vs Ano Anterior:",
        "anomalias": "Dados para identificação de anomalias:",
        "observacoes_finais": "Resumo dos dados do mês:",
        "oficina": "Dados da oficina para análise:",
        "volume_completo": "📊 Dados de volume e variações por modelo:",
        "conclusoes": "💡 Dados para conclusões e recomendações:",
    },
    "en": {
        "analise_volume": "Volume data for analysis:",
        "variacoes_modelo": "Model variation data:",
        "comparativos": "Comparative data (Actual vs Flex, Previous Month, Budget, Previous Year):",
        "real_vs_mes_anterior": "Actual vs Previous Month data:",
        "real_vs_flex": "Actual vs Flex Budget data:",
        "real_vs_budget": "Actual vs Budget data:",
        "real_vs_ano_anterior": "Actual vs Previous Year data:",
        "anomalias": "Data for anomaly detection:",
        "observacoes_finais": "Month data summary:",
        "oficina": "Shop data for analysis:",
        "volume_completo": "📊 Volume and model variation data:",
        "conclusoes": "💡 Data for conclusions and recommendations:",
    },
}

_FALLBACK_NOTICE = {
    "pt-BR": (
        "\n\n[Nota: Texto gerado automaticamente sem IA. "
        "Configure a chave OpenAI para análises contextuais detalhadas.]"
    ),
    "en": (
        "\n\n[Note: Auto-generated text without AI. "
        "Configure the OpenAI key for detailed contextual analyses.]"
    ),
}


def _fallback_texto(
    tipo_secao: str,
    dados: str,
    mes: str,
    ano: int,
    idioma: str,
) -> str:
    """Gera texto fallback simples quando a LLM não está disponível."""
    headers = _FALLBACK_HEADERS.get(idioma, _FALLBACK_HEADERS["pt-BR"])
    header = headers.get(tipo_secao, f"Dados ({tipo_secao}):")
    notice = _FALLBACK_NOTICE.get(idioma, _FALLBACK_NOTICE["pt-BR"])
    return f"{header}\n\n{dados}{notice}"
