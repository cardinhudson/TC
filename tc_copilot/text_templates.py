"""
TC Copilot — Geração de textos analíticos via templates Python (sem API).

Substitui a LLM por narrativa automática baseada em regras condicionais.
Reutiliza os mesmos dados calculados por data_collector.calcular_variacoes()
e as funções formatar_dados_*().

Cada função retorna uma string Markdown com a mesma estrutura que a LLM produziria.
"""

from __future__ import annotations

from typing import Any


# ═══════════════════════════════════════════════════════════════
#  HELPERS DE FORMATAÇÃO (reutiliza do data_collector)
# ═══════════════════════════════════════════════════════════════

def _fmt(valor: float, decimais: int = 2) -> str:
    """Formata número no padrão pt-BR: 1.234.567,89"""
    import pandas as pd
    if pd.isna(valor) or valor is None:
        return "0"
    try:
        s = f"{float(valor):,.{decimais}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(valor)


def _fmt_k(valor: float, decimais: int = 1) -> str:
    """Formata valor em kBRL (÷1000)."""
    import pandas as pd
    if pd.isna(valor) or valor is None:
        return "0 kBRL"
    try:
        v = float(valor) / 1000
        s = f"{v:,.{decimais}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".") + " kBRL"
    except (ValueError, TypeError):
        return str(valor)


def _fmt_cpu(valor: float) -> str:
    """Formata valor como R$/veíc."""
    import pandas as pd
    if pd.isna(valor) or valor is None:
        return "0 R$/veíc"
    try:
        v = float(valor)
        s = f"{v:,.1f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".") + " R$/veíc"
    except (ValueError, TypeError):
        return str(valor)


def _pct(atual: float, anterior: float) -> str:
    """Variação percentual formatada."""
    if anterior == 0:
        return "sem ref." if atual == 0 else "sem base (ref.=0)"
    var = (atual - anterior) / abs(anterior) * 100
    sinal = "+" if var > 0 else ""
    return f"{sinal}{var:.1f}%"


def _cpu(custo: float, volume: float) -> float:
    """CPU = custo / volume. Retorna 0 se volume <= 0."""
    if not volume or volume <= 0:
        return 0.0
    return custo / volume


def _sinal(val: float) -> str:
    return "+" if val >= 0 else ""


def _cor(val: float, inversao: bool = False) -> str:
    """Emoji de cor. inversao=True → Δ+ é bom (ex: volume); False → Δ+ é ruim (ex: custo)."""
    if val == 0:
        return "⚪"
    if inversao:
        return "🟢" if val > 0 else "🔴"
    return "🔴" if val > 0 else "🟢"


def _cor_custo(val: float) -> str:
    """Para custos: Δ negativo = 🟢 (economia), Δ positivo = 🔴 (aumento)."""
    return _cor(val, inversao=False)


def _cor_vol(val: float) -> str:
    """Para volumes: Δ positivo = 🟢, Δ negativo = 🔴."""
    return _cor(val, inversao=True)


def _verbo_custo(delta: float) -> tuple[str, str]:
    """Retorna (verbo, adjetivo) para variação de custo."""
    if delta > 0:
        return ("aumentou", "desfavorável")
    elif delta < 0:
        return ("reduziu", "favorável")
    return ("manteve-se estável", "neutro")


def _verbo_vol(delta: float) -> tuple[str, str]:
    """Retorna (verbo, adjetivo) para variação de volume."""
    if delta > 0:
        return ("superou", "positivo")
    elif delta < 0:
        return ("ficou abaixo", "negativo")
    return ("igualou", "neutro")


# ═══════════════════════════════════════════════════════════════
#  SEÇÃO 0 — RESUMO EXECUTIVO
# ═══════════════════════════════════════════════════════════════

def gerar_texto_resumo_executivo(
    variacoes: dict,
    dados_graficos: dict | None = None,
    mes_nome: str = "",
    ano: int = 0,
    oficinas_resumo: list[tuple[str, float, float]] | None = None,
) -> str:
    """
    Gera texto do resumo executivo com 6 parágrafos temáticos.

    Args:
        variacoes: Dict retornado por calcular_variacoes()
        dados_graficos: Dados de gráficos (wf_budget_labels/values, etc.)
        mes_nome: Nome do mês
        ano: Ano do relatório
        oficinas_resumo: Lista de (nome, fp_real, delta_vs_bud) para top oficinas
    """
    v = variacoes["volume"]
    fp = variacoes["custo_fp"]
    vol_real = v["real"]
    vol_bud = v["budget"]
    vol_ant = v["mes_anterior"]
    fp_real = fp["real"]
    fp_bud = fp.get("budget", 0)
    fp_flex = fp.get("flex", 0)
    fp_ant = fp.get("mes_anterior", 0)

    delta_vol_bud = vol_real - vol_bud
    delta_vol_ant = vol_real - vol_ant
    efeito_vol = fp_flex - fp_bud
    efeito_op = fp_real - fp_flex
    delta_bud = fp_real - fp_bud

    cpu_real = _cpu(fp_real, vol_real)
    cpu_bud = _cpu(fp_bud, vol_bud)
    cpu_flex = _cpu(fp_flex, vol_real)

    paragrafos = []

    # ── 1. Volume ──
    v_verbo, v_adj = _verbo_vol(delta_vol_bud)
    p1 = (
        f"No mês de {mes_nome}/{ano}, o volume real de produção atingiu "
        f"**{_fmt(vol_real, 0)} unidades**, o que {v_verbo} o Budget de "
        f"{_fmt(vol_bud, 0)} un. em {_sinal(delta_vol_bud)}{_fmt(abs(delta_vol_bud), 0)} un. "
        f"({_pct(vol_real, vol_bud)}). "
    )
    if vol_ant > 0:
        v2_verbo, _ = _verbo_vol(delta_vol_ant)
        p1 += (
            f"Em relação ao mês anterior, o volume {v2_verbo} "
            f"({_sinal(delta_vol_ant)}{_fmt(abs(delta_vol_ant), 0)} un., {_pct(vol_real, vol_ant)})."
        )
    paragrafos.append(p1)

    # ── 2. Custo FP / Waterfall ──
    v_imp = "negativamente" if efeito_vol > 0 else "positivamente"
    op_verbo, op_adj = _verbo_custo(efeito_op)
    p2 = (
        f"O Custo FP Real totalizou **{_fmt_k(fp_real)}** ({_fmt_cpu(cpu_real)}), "
        f"contra um Budget de {_fmt_k(fp_bud)} ({_fmt_cpu(cpu_bud)}), "
        f"resultando em um delta de {_sinal(delta_bud)}{_fmt_k(delta_bud)} ({_pct(fp_real, fp_bud)}). "
        f"O efeito volume impactou {v_imp} em {_sinal(efeito_vol)}{_fmt_k(efeito_vol)}, "
        f"levando o Flex Budget a {_fmt_k(fp_flex)} ({_fmt_cpu(cpu_flex)}). "
        f"O efeito operacional (preço e mix) {op_verbo} {_fmt_k(abs(efeito_op))}, "
        f"indicando performance **{op_adj}** frente ao esperado."
    )
    paragrafos.append(p2)

    # ── 3. CPU por modelo ──
    cpu_modelos_real = variacoes["cpu_modelos"].get("real", {})
    cpu_modelos_bud = variacoes["cpu_modelos"].get("budget", {})
    if cpu_modelos_real:
        diffs = []
        for m in cpu_modelos_real:
            r = cpu_modelos_real[m]
            b = cpu_modelos_bud.get(m, 0)
            diffs.append((m, r, b, r - b))
        diffs.sort(key=lambda x: abs(x[3]), reverse=True)
        top3 = diffs[:3]
        items = []
        for m, r, b, d in top3:
            items.append(f"{m} ({_fmt_cpu(r)}, Δ {_sinal(d)}{_fmt_cpu(d)})")
        p3 = (
            f"Em termos de CPU (R$/veíc), os modelos com maior desvio vs Budget foram: "
            f"{'; '.join(items)}."
        )
        paragrafos.append(p3)

    # ── 4. Type 05 ──
    graf_global = dados_graficos.get("global", {}) if dados_graficos else {}
    wf_labels = graf_global.get("wf_budget_labels", [])
    wf_values = graf_global.get("wf_budget_values", [])
    if len(wf_labels) > 4:
        # Pegar as categorias intermediárias (excluir BUD, Flex, Real e Outros)
        cats = list(zip(wf_labels[2:-1], wf_values[2:-1]))
        cats.sort(key=lambda x: abs(x[1]), reverse=True)
        top_cats = cats[:3]
        items_t05 = []
        for lbl, val in top_cats:
            lbl_clean = lbl.replace("\n", " ").strip()
            items_t05.append(f"{_cor_custo(val)} {lbl_clean} ({_sinal(val)}{_fmt_cpu(val)})")
        p4 = (
            f"As categorias de custo com maior impacto no waterfall Budget (CPU) foram: "
            f"{', '.join(items_t05)}."
        )
        paragrafos.append(p4)

    # ── 5. Oficinas ──
    if oficinas_resumo:
        oficinas_resumo.sort(key=lambda x: abs(x[2]), reverse=True)
        top_ofc = oficinas_resumo[:3]
        items_ofc = []
        for nome, fp_r, delta in top_ofc:
            items_ofc.append(
                f"{_cor_custo(delta)} {nome} ({_fmt_k(fp_r)}, Δ {_sinal(delta)}{_fmt_k(delta)})"
            )
        p5 = f"As oficinas com maiores desvios vs Budget foram: {', '.join(items_ofc)}."
        paragrafos.append(p5)

    # ── 6. Alertas ──
    alertas = []
    if abs(delta_vol_bud) / max(vol_bud, 1) > 0.10:
        alertas.append(
            f"⚠️ Volume desviou {_pct(vol_real, vol_bud)} do Budget — variação significativa."
        )
    if fp_bud > 0 and abs(delta_bud) / fp_bud > 0.10:
        alertas.append(
            f"⚠️ Custo FP desviou {_pct(fp_real, fp_bud)} do Budget — atenção requerida."
        )
    if abs(efeito_op) > 0 and fp_flex > 0 and abs(efeito_op) / fp_flex > 0.05:
        alertas.append(
            f"⚠️ Efeito operacional de {_sinal(efeito_op)}{_fmt_k(efeito_op)} "
            f"({_pct(fp_real, fp_flex)} vs Flex) — investigar causas de preço/mix."
        )
    if variacoes.get("sem_ano_anterior"):
        alertas.append("⚠️ Dados do ano anterior não disponíveis — comparação YoY prejudicada.")

    if alertas:
        paragrafos.append("**Alertas e pontos de atenção:**\n" + "\n".join(f"- {a}" for a in alertas))
    else:
        paragrafos.append("Não foram identificados alertas significativos neste período.")

    return "\n\n".join(paragrafos)


# ═══════════════════════════════════════════════════════════════
#  SEÇÃO 1 — VOLUME E VARIAÇÕES POR MODELO
# ═══════════════════════════════════════════════════════════════

def gerar_texto_volume_completo(
    variacoes: dict,
    mes_nome: str = "",
    ano: int = 0,
    ano_anterior: int = 0,
) -> str:
    """
    Gera texto analítico para a seção de volume (4 sub-tópicos).
    """
    v = variacoes["volume"]
    modelos = variacoes["variacao_modelos"]
    vol_real = v["real"]
    vol_bud = v["budget"]
    vol_ant = v["mes_anterior"]
    vol_ano_ant = v.get("ano_anterior", 0)

    partes = []

    # ── 1.1 Volume Total ──
    partes.append(f"### 1.1 Volume Total — {mes_nome}/{ano}")
    partes.append(
        f"O volume real de produção em {mes_nome} foi de **{_fmt(vol_real, 0)} unidades**. "
        f"O volume Actual (projeção) era de {_fmt(v.get('actual', 0), 0)} un. "
        f"e o Budget de {_fmt(vol_bud, 0)} un."
    )

    # ── 1.2 Real vs Budget ──
    delta_bud = vol_real - vol_bud
    v_verbo, _ = _verbo_vol(delta_bud)
    partes.append(f"\n### 1.2 Real vs Budget")
    partes.append(
        f"O volume real {v_verbo} o Budget em "
        f"**{_sinal(delta_bud)}{_fmt(abs(delta_bud), 0)} unidades** ({_pct(vol_real, vol_bud)})."
    )
    # Top 10 modelos
    modelos_bud = sorted(modelos.items(), key=lambda x: abs(x[1]["var_budget"]), reverse=True)[:10]
    if modelos_bud:
        partes.append("\n**Top 10 modelos por impacto (Real vs Budget):**")
        for nome, info in modelos_bud:
            d = info["var_budget"]
            pct = info.get("pct_budget")
            pct_str = f"{_sinal(pct)}{pct:.1f}%" if pct is not None else "N/A"
            partes.append(
                f"- {_cor_vol(d)} **{nome}**: {_fmt(info['vol_real'], 0)} un. "
                f"(Δ {_sinal(d)}{_fmt(abs(d), 0)} un., {pct_str})"
            )

    # ── 1.3 Real vs Mês Anterior ──
    delta_ant = vol_real - vol_ant
    v_verbo_ant, _ = _verbo_vol(delta_ant)
    partes.append(f"\n### 1.3 Real vs Mês Anterior")
    if vol_ant > 0:
        partes.append(
            f"O volume {v_verbo_ant} o mês anterior em "
            f"**{_sinal(delta_ant)}{_fmt(abs(delta_ant), 0)} un.** ({_pct(vol_real, vol_ant)})."
        )
        modelos_ant = sorted(modelos.items(), key=lambda x: abs(x[1]["var_mes_ant"]), reverse=True)[:10]
        if modelos_ant:
            partes.append("\n**Top 10 modelos por impacto (Real vs Mês Anterior):**")
            for nome, info in modelos_ant:
                d = info["var_mes_ant"]
                pct = info.get("pct_mes_ant")
                pct_str = f"{_sinal(pct)}{pct:.1f}%" if pct is not None else "N/A"
                partes.append(
                    f"- {_cor_vol(d)} **{nome}**: {_fmt(info['vol_real'], 0)} un. "
                    f"(Δ {_sinal(d)}{_fmt(abs(d), 0)} un., {pct_str})"
                )
    else:
        partes.append("⚠️ Mês anterior sem dados de volume para comparação.")

    # ── 1.4 Real vs Ano Anterior ──
    partes.append(f"\n### 1.4 Real vs Mesmo Mês de {ano_anterior}")
    if vol_ano_ant > 0:
        delta_yoy = vol_real - vol_ano_ant
        v_verbo_yoy, _ = _verbo_vol(delta_yoy)
        partes.append(
            f"O volume {v_verbo_yoy} o mesmo mês de {ano_anterior} em "
            f"**{_sinal(delta_yoy)}{_fmt(abs(delta_yoy), 0)} un.** ({_pct(vol_real, vol_ano_ant)})."
        )
        modelos_yoy = sorted(modelos.items(), key=lambda x: abs(x[1].get("var_ano_ant", 0)), reverse=True)[:10]
        has_yoy = any(info.get("vol_ano_ant", 0) != 0 for _, info in modelos_yoy)
        if has_yoy:
            partes.append(f"\n**Top 10 modelos por impacto (Real vs {ano_anterior}):**")
            for nome, info in modelos_yoy:
                d = info.get("var_ano_ant", 0)
                pct = info.get("pct_ano_ant")
                pct_str = f"{_sinal(pct)}{pct:.1f}%" if pct is not None else "N/A"
                partes.append(
                    f"- {_cor_vol(d)} **{nome}**: {_fmt(info['vol_real'], 0)} un. "
                    f"(Δ {_sinal(d)}{_fmt(abs(d), 0)} un., {pct_str})"
                )
    else:
        partes.append(f"⚠️ Sem dados de volume de {ano_anterior} para comparação.")

    return "\n".join(partes)


# ═══════════════════════════════════════════════════════════════
#  SEÇÃO 2 — COMPARATIVOS
# ═══════════════════════════════════════════════════════════════

def gerar_texto_comparativos(
    dados_formatados: str,
    variacoes: dict,
    mes_nome: str = "",
    ano: int = 0,
    ano_anterior: int = 0,
) -> str:
    """
    Gera texto analítico para os 3 comparativos.

    Recebe dados_formatados (output de formatar_dados_comparativos_agrupado)
    para incluir o drill-down detalhado. Envelopa com parágrafos narrativos.
    """
    fp = variacoes["custo_fp"]
    v = variacoes["volume"]
    fp_real = fp["real"]
    fp_bud = fp.get("budget", 0)
    fp_flex = fp.get("flex", 0)
    fp_ant = fp.get("mes_anterior", 0)
    vol_real = v["real"]
    vol_bud = v["budget"]

    cpu_real = _cpu(fp_real, vol_real)
    cpu_bud = _cpu(fp_bud, vol_bud)
    cpu_flex = _cpu(fp_flex, vol_real)

    efeito_vol = fp_flex - fp_bud
    efeito_op = fp_real - fp_flex
    delta_total = fp_real - fp_bud

    partes = []

    # ── 2.1 Real vs Budget (Efeito Flex Volume) ──
    partes.append("### 2.1 Real vs Budget (Efeito Flex Volume)")
    rel_vol = "superou" if vol_real > vol_bud else "ficou abaixo do"
    imp_vol = "negativamente" if efeito_vol > 0 else "positivamente"
    imp_op = "gerou economia" if efeito_op < 0 else "gerou aumento"
    perf_op = "melhor" if efeito_op < 0 else "pior"

    partes.append(
        f"O Custo FP Real de **{_fmt_k(fp_real)}** ({_fmt_cpu(cpu_real)}) "
        f"compara-se ao Budget de {_fmt_k(fp_bud)} ({_fmt_cpu(cpu_bud)}), "
        f"com delta total de {_sinal(delta_total)}{_fmt_k(delta_total)} ({_pct(fp_real, fp_bud)})."
    )
    partes.append(
        f"\nO volume real de {_fmt(vol_real, 0)} un. {rel_vol} Budget de "
        f"{_fmt(vol_bud, 0)} un. ({_pct(vol_real, vol_bud)}), impactando {imp_vol} "
        f"o custo em {_sinal(efeito_vol)}{_fmt_k(efeito_vol)}. "
        f"O Flex Budget resultante ficou em {_fmt_k(fp_flex)} ({_fmt_cpu(cpu_flex)}). "
        f"O efeito operacional {imp_op} de {_fmt_k(abs(efeito_op))}, "
        f"indicando performance {perf_op} do que o esperado."
    )

    # ── 2.2 Real vs Mês Anterior ──
    partes.append("\n### 2.2 Real vs Mês Anterior")
    if fp_ant > 0:
        delta_ant = fp_real - fp_ant
        v_ant, adj_ant = _verbo_custo(delta_ant)
        cpu_ant = _cpu(fp_ant, v["mes_anterior"])
        partes.append(
            f"O Custo FP Real de {_fmt_k(fp_real)} ({_fmt_cpu(cpu_real)}) "
            f"{v_ant} em {_fmt_k(abs(delta_ant))} ({_pct(fp_real, fp_ant)}) "
            f"em relação ao mês anterior de {_fmt_k(fp_ant)} ({_fmt_cpu(cpu_ant)}). "
            f"Essa variação é considerada **{adj_ant}**."
        )
    else:
        partes.append("⚠️ Mês anterior sem dados de custo para comparação.")

    # ── 2.3 Real vs Ano Anterior ──
    partes.append(f"\n### 2.3 Real vs Mesmo Mês de {ano_anterior}")
    if variacoes.get("sem_ano_anterior"):
        partes.append(f"⚠️ Sem dados de {ano_anterior} para comparação neste período.")
    else:
        partes.append(
            f"Comparação disponível no drill-down detalhado abaixo."
        )

    # ── Incluir drill-down completo gerado pelo data_collector ──
    partes.append("\n---\n")
    partes.append("**Detalhamento por Type 05 → Type 06 → Account:**")
    partes.append("")
    partes.append(dados_formatados)

    return "\n".join(partes)


# ═══════════════════════════════════════════════════════════════
#  SEÇÃO 3 — CONCLUSÕES E RECOMENDAÇÕES
# ═══════════════════════════════════════════════════════════════

def gerar_texto_conclusoes(
    variacoes: dict,
    mes_nome: str = "",
    ano: int = 0,
) -> str:
    """
    Gera conclusões, alertas e recomendações baseados nos dados.
    """
    v = variacoes["volume"]
    fp = variacoes["custo_fp"]
    modelos = variacoes["variacao_modelos"]
    vol_real = v["real"]
    vol_bud = v["budget"]
    vol_ant = v["mes_anterior"]
    fp_real = fp["real"]
    fp_bud = fp.get("budget", 0)
    fp_flex = fp.get("flex", 0)
    fp_ant = fp.get("mes_anterior", 0)

    partes = []

    # ── Alertas / Anomalias ──
    partes.append("### Alertas e Anomalias")
    alertas = []

    # Volume
    if vol_bud > 0 and abs(vol_real - vol_bud) / vol_bud > 0.10:
        direcao = "acima" if vol_real > vol_bud else "abaixo"
        alertas.append(
            f"- ⚠️ Volume real ficou **{_pct(vol_real, vol_bud)}** {direcao} do Budget — "
            f"variação significativa que impacta diretamente o Flex."
        )

    # Custo FP vs Flex
    if fp_flex > 0 and abs(fp_real - fp_flex) / fp_flex > 0.05:
        direcao = "acima" if fp_real > fp_flex else "abaixo"
        alertas.append(
            f"- ⚠️ Custo FP Real ficou **{_pct(fp_real, fp_flex)}** {direcao} do Flex Budget — "
            f"efeito operacional relevante."
        )

    # Custo FP vs Mês Anterior
    if fp_ant > 0 and abs(fp_real - fp_ant) / fp_ant > 0.15:
        direcao = "aumento" if fp_real > fp_ant else "redução"
        alertas.append(
            f"- ⚠️ {direcao.capitalize()} de **{_pct(fp_real, fp_ant)}** no Custo FP vs mês anterior — "
            f"requer investigação."
        )

    # Modelos com variação extrema
    for nome, info in sorted(modelos.items(), key=lambda x: abs(x[1]["var_budget"]), reverse=True)[:3]:
        pct = info.get("pct_budget")
        if pct is not None and abs(pct) > 30:
            alertas.append(
                f"- ⚠️ Modelo **{nome}**: variação de volume de {_sinal(pct)}{pct:.0f}% vs Budget."
            )

    if variacoes.get("sem_ano_anterior"):
        alertas.append("- ⚠️ Dados do ano anterior indisponíveis — análise YoY prejudicada.")

    if alertas:
        partes.extend(alertas)
    else:
        partes.append("Nenhum alerta significativo identificado neste período.")

    # ── Aprendizados ──
    partes.append("\n### Aprendizados do Período")
    efeito_op = fp_real - fp_flex
    if efeito_op < 0:
        partes.append(
            f"- O efeito operacional foi **favorável** em {_fmt_k(abs(efeito_op))}, "
            f"sugerindo boa gestão de preço e mix no período."
        )
    elif efeito_op > 0:
        partes.append(
            f"- O efeito operacional foi **desfavorável** em {_sinal(efeito_op)}{_fmt_k(efeito_op)}, "
            f"indicando necessidade de revisão de preços/mix de produção."
        )

    if vol_ant > 0:
        delta_vol_ant = vol_real - vol_ant
        if abs(delta_vol_ant) > 0:
            partes.append(
                f"- Volume variou {_sinal(delta_vol_ant)}{_fmt(abs(delta_vol_ant), 0)} un. "
                f"({_pct(vol_real, vol_ant)}) vs mês anterior — "
                f"{'tendência de crescimento' if delta_vol_ant > 0 else 'tendência de redução'}."
            )

    # ── Recomendações ──
    partes.append("\n### Recomendações")
    recomendacoes = []

    if fp_flex > 0 and fp_real > fp_flex:
        recomendacoes.append(
            "- Investigar drivers do efeito operacional desfavorável "
            "nos maiores Type 05/Type 06 do drill-down."
        )

    if vol_bud > 0 and abs(vol_real - vol_bud) / vol_bud > 0.10:
        recomendacoes.append(
            "- Revisar premissas de volume do Budget para os próximos meses."
        )

    recomendacoes.append(
        "- Acompanhar evolução dos top modelos com maior desvio de CPU."
    )
    recomendacoes.append(
        "- Monitorar oficinas com maiores deltas para ações corretivas."
    )

    partes.extend(recomendacoes)

    return "\n".join(partes)


# ═══════════════════════════════════════════════════════════════
#  SEÇÃO 4 — OFICINA
# ═══════════════════════════════════════════════════════════════

def gerar_texto_oficina(
    ofc_dict: dict[str, str],
    ofc_nome: str,
    mes_nome: str = "",
    ano: int = 0,
) -> str:
    """
    Gera texto analítico para uma oficina.

    Recebe o dict de formatar_dados_oficina() com sub-seções já prontas
    e adiciona uma síntese narrativa.

    Args:
        ofc_dict: Dict com chaves resumo, budget_flex, mes_anterior, ano_anterior
        ofc_nome: Nome da oficina
    """
    partes = []

    partes.append(f"**Oficina {ofc_nome} — {mes_nome}/{ano}**\n")

    # Resumo pré-formatado pelo data_collector
    partes.append(ofc_dict.get("resumo", ""))
    partes.append("")

    # Sub-seções de comparativo (já com drill-down)
    sub_secoes = [
        ("budget_flex", "Real vs Budget (Efeito Flex Volume)"),
        ("mes_anterior", "Real vs Mês Anterior"),
        ("ano_anterior", "Real vs Ano Anterior"),
    ]
    for chave, titulo in sub_secoes:
        conteudo = ofc_dict.get(chave, "")
        if conteudo:
            partes.append(f"**{titulo}:**\n")
            partes.append(conteudo)
            partes.append("")

    # Síntese automática
    resumo_text = ofc_dict.get("resumo", "")
    if "🔴" in resumo_text:
        partes.append(
            f"A oficina **{ofc_nome}** apresenta desvios desfavoráveis que requerem "
            f"acompanhamento. Recomenda-se investigar os Type 06 com maiores deltas no drill-down acima."
        )
    elif "🟢" in resumo_text:
        partes.append(
            f"A oficina **{ofc_nome}** apresenta resultados favoráveis no período, "
            f"com economias nos principais Type 05."
        )
    else:
        partes.append(
            f"A oficina **{ofc_nome}** manteve-se dentro dos parâmetros esperados."
        )

    return "\n".join(partes)


# ═══════════════════════════════════════════════════════════════
#  ORQUESTRADOR — GERA TODAS AS SEÇÕES SEM API
# ═══════════════════════════════════════════════════════════════

def gerar_todas_secoes_local(
    dados: dict,
    variacoes: dict,
    dados_graficos: dict | None = None,
    oficinas_info: dict[str, dict] | None = None,
    idioma: str = "pt-BR",
) -> dict[str, str]:
    """
    Gera todas as seções do relatório sem usar API.

    Args:
        dados: Dict retornado por coletar_dados_mes()
        variacoes: Dict retornado por calcular_variacoes()
        dados_graficos: Dados de gráficos waterfall
        oficinas_info: Dict {nome_oficina: ofc_dict} de formatar_dados_oficina()
        idioma: Idioma do relatório

    Returns:
        Dict {tipo_secao: texto} no mesmo formato que gerar_relatorio_mes() produz
    """
    from tc_copilot.data_collector import (
        formatar_dados_comparativos_agrupado,
    )

    mes_nome = dados["mes_nome"]
    ano = dados["ano"]
    ano_anterior = dados.get("ano_anterior", ano - 1)

    secoes: dict[str, str] = {}

    # ── Volume Completo ──
    secoes["volume_completo"] = gerar_texto_volume_completo(
        variacoes, mes_nome=mes_nome, ano=ano, ano_anterior=ano_anterior,
    )

    # ── Comparativos (com drill-down detalhado do data_collector) ──
    dados_comp = formatar_dados_comparativos_agrupado(dados, variacoes)
    secoes["comparativos"] = gerar_texto_comparativos(
        dados_comp, variacoes,
        mes_nome=mes_nome, ano=ano, ano_anterior=ano_anterior,
    )

    # ── Conclusões ──
    secoes["conclusoes"] = gerar_texto_conclusoes(
        variacoes, mes_nome=mes_nome, ano=ano,
    )

    # ── Oficinas ──
    oficinas_resumo = []
    if oficinas_info:
        for ofc_nome, ofc_dict in oficinas_info.items():
            secoes[f"oficina_{ofc_nome}"] = gerar_texto_oficina(
                ofc_dict, ofc_nome, mes_nome=mes_nome, ano=ano,
            )
            # Extrair delta para o resumo executivo
            # (parse simples do texto de resumo)
            try:
                from tc_copilot.data_collector import (
                    _filtrar_por_oficina,
                    _safe_sum,
                )
                dados_ofc = _filtrar_por_oficina(dados, ofc_nome)
                fp_real_ofc = _safe_sum(dados_ofc.get("custo_real"), "Custo FP")
                fp_bud_ofc = _safe_sum(dados_ofc.get("custo_bud"), "Custo FP")
                oficinas_resumo.append((ofc_nome, fp_real_ofc, fp_real_ofc - fp_bud_ofc))
            except Exception:
                pass

    # ── Resumo Executivo (gerado por último — resume tudo) ──
    secoes["resumo_executivo"] = gerar_texto_resumo_executivo(
        variacoes,
        dados_graficos=dados_graficos,
        mes_nome=mes_nome,
        ano=ano,
        oficinas_resumo=oficinas_resumo if oficinas_resumo else None,
    )

    return secoes
