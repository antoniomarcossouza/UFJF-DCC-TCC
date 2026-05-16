"""Textos de interpretação automática (functional core)."""

from __future__ import annotations

from dashboard.utils.formatting import fmt_pct


def insight_yoy(
    arrecadado_atual: float | None,
    arrecadado_anterior: float | None,
    *,
    ano_atual: int,
    ano_anterior: int,
) -> str:
    """Resumo de variação ano a ano; mensagem neutra se comparação inválida."""
    if (
        arrecadado_atual is None
        or arrecadado_anterior is None
        or arrecadado_anterior == 0
    ):
        return (
            "Comparação com o ano anterior indisponível para o período "
            "selecionado (dados insuficientes ou base zerada)."
        )
    variacao = (
        (arrecadado_atual - arrecadado_anterior) / arrecadado_anterior * 100.0
    )
    pct_txt = fmt_pct(abs(variacao), decimals=1)
    if variacao > 0:
        return (
            f"A arrecadação cresceu {pct_txt} em relação ao mesmo período de "
            f"{ano_anterior} (comparando com {ano_atual})."
        )
    if variacao < 0:
        return (
            f"A arrecadação caiu {pct_txt} em relação ao mesmo período de "
            f"{ano_anterior} (comparando com {ano_atual})."
        )
    return (
        f"A arrecadação ficou estável em relação ao mesmo período de "
        f"{ano_anterior}."
    )


def insight_principal_fonte(origem_label: str, pct: float | None) -> str:
    if pct is None:
        return f"A principal fonte agregada é: {origem_label}."
    return (
        f"A principal fonte de receita é {origem_label}, com "
        f"{fmt_pct(pct, decimals=1)} do total arrecadado no período."
    )


def insight_execucao(
    realizado: float | None,
    previsto: float | None,
    pct: float | None,
) -> str:
    if pct is None or realizado is None or previsto is None:
        return (
            "Não foi possível calcular o percentual de execução da previsão "
            "com os filtros atuais."
        )
    return (
        f"O município arrecadou {fmt_pct(pct, decimals=1)} da "
        f"previsão atualizada até o momento (realizado sobre meta anual "
        "acumulada no recorte)."
    )


def insight_participacao_transferencias(
    pct_uniao: float | None,
    pct_estados: float | None,
) -> str:
    if pct_uniao is None and pct_estados is None:
        return "Participação de repasses não calculada (dados insuficientes)."
    pu = fmt_pct(pct_uniao, decimals=1) if pct_uniao is not None else "—"
    pe = fmt_pct(pct_estados, decimals=1) if pct_estados is not None else "—"
    return (
        f"Repasse federal (agregado): {pu} do arrecadado; "
        f"repasse estadual: {pe}."
    )


def insight_evolucao_anual(
    valores_por_ano: dict[int, float],
    *,
    ano_parcial: int | None = None,
) -> str:
    """Texto curto a partir de totais por ano (ex.: série 2024–2026)."""
    if len(valores_por_ano) < 2:
        return "Série histórica curta demais para comparar anos."
    anos = sorted(valores_por_ano)
    a0, a1 = anos[0], anos[-1]
    v0, v1 = valores_por_ano[a0], valores_por_ano[a1]
    if v0 == 0:
        return f"Total em {a1}: {fmt_pct(100.0, decimals=0)} da base inicial."
    if a0 == a1:
        return f"Arrecadação total em {a1} registrada nos dados."
    variacao = (v1 - v0) / v0 * 100.0
    if variacao > 0:
        tendencia = "cresceu"
    elif variacao < 0:
        tendencia = "caiu"
    else:
        tendencia = "manteve"
    extra = ""
    if ano_parcial is not None and ano_parcial in valores_por_ano:
        extra = f" O ano {ano_parcial} pode estar parcial no gráfico."
    return (
        f"Entre {a0} e {a1}, a arrecadação total {tendencia} "
        f"{fmt_pct(abs(variacao), decimals=1)}.{extra}"
    )
