"""Textos de interpretação automática (functional core)."""

from __future__ import annotations

from datetime import date

from dashboard.utils.formatting import fmt_brl_compact, fmt_pct


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


def insight_saldo_fiscal(
    arrecadacao: float | None,
    pagamentos: float | None,
    saldo: float | None,
) -> str:
    """Resumo se arrecadação cobre pagamentos no recorte."""
    if arrecadacao is None or pagamentos is None or saldo is None:
        return (
            "Saldo fiscal não calculado: faltam arrecadação, pagamentos ou "
            "ambos no recorte dos filtros."
        )
    if arrecadacao == 0 and pagamentos == 0:
        return "Arrecadação e pagamentos zerados neste recorte."
    if saldo >= 0:
        return (
            "No recorte, a arrecadação cobre os pagamentos: saldo "
            f"{fmt_brl_compact(saldo)} (entrou mais do que saiu em valor "
            "consolidado)."
        )
    return (
        "No recorte, os pagamentos superam a arrecadação: saldo "
        f"{fmt_brl_compact(saldo)} (saída maior que entrada consolidada)."
    )


def insight_saldo_ultimo_mes(
    saldo_mes: float | None,
    dt_ref: object | None,
) -> str:
    """Interpreta saldo do último mês com dado."""
    if saldo_mes is None:
        return "Saldo do último mês indisponível."
    ref_txt = "—"
    if dt_ref is not None:
        if isinstance(dt_ref, date):
            ref_txt = dt_ref.strftime("%m/%Y")
        elif hasattr(dt_ref, "strftime"):
            ref_txt = str(dt_ref.strftime("%m/%Y"))  # type: ignore[union-attr]
    if saldo_mes >= 0:
        return (
            f"No último mês com dado ({ref_txt}), arrecadação ≥ pagamentos "
            f"(saldo mensal {fmt_brl_compact(saldo_mes)})."
        )
    return (
        f"No último mês com dado ({ref_txt}), pagamentos > arrecadação "
        f"(saldo mensal {fmt_brl_compact(saldo_mes)})."
    )


def insight_tendencia_saldo(saldos_mensais: list[float]) -> str:
    """Conta meses com saldo negativo na série mensal."""
    if not saldos_mensais:
        return "Sem série de saldo mensal para analisar."
    neg = sum(1 for s in saldos_mensais if s < 0)
    total = len(saldos_mensais)
    if total == 0:
        return "Sem série de saldo mensal para analisar."
    if neg == 0:
        return (
            f"Em todos os {total} mês(es) da série, o saldo mensal foi "
            "≥ 0 (arrecadação cobriu pagamentos mês a mês)."
        )
    if neg == total:
        return (
            f"Em todos os {total} mês(es) da série, o saldo mensal foi "
            "negativo (pagamentos superaram arrecadação mês a mês)."
        )
    pct_neg = 100.0 * neg / total
    return (
        f"{neg} de {total} mês(es) com saldo negativo "
        f"({fmt_pct(pct_neg, decimals=1)} da série)."
    )


def insight_top_fornecedor(
    nome: str | None,
    vl_pago: float | None,
    pct_total: float | None,
) -> str:
    if not nome or not str(nome).strip():
        return "Fornecedor líder não identificado neste recorte."
    nm = str(nome).strip()
    if vl_pago is None:
        return f"Principal fornecedor por valor: {nm}."
    vtxt = fmt_brl_compact(vl_pago)
    if pct_total is None:
        return f"O fornecedor que mais recebeu pagamentos foi {nm} ({vtxt})."
    return (
        f"O fornecedor que mais recebeu foi {nm}, com {vtxt} "
        f"({fmt_pct(pct_total, decimals=1)} do total pago no ranking)."
    )


def insight_concentracao_pareto(pct_top_n: float | None, n: int) -> str:
    """Risco de dependência quando poucos concentram muito."""
    if pct_top_n is None or n <= 0:
        return "Concentração nos maiores fornecedores não calculada."
    return (
        f"Os {n} maiores fornecedores somam {fmt_pct(pct_top_n, decimals=1)} "
        "do total pago no recorte — alta concentração pode indicar "
        "dependência de poucos credores."
    )


def insight_top_unidade(
    nome: str | None,
    vl_pago: float | None,
    pct_total: float | None,
) -> str:
    if not nome or not str(nome).strip():
        return "Unidade com maior execução não identificada."
    nm = str(nome).strip()
    if vl_pago is None:
        return f"A unidade que mais pagou no recorte foi: {nm}."
    vtxt = fmt_brl_compact(vl_pago)
    if pct_total is None:
        return f"A unidade com maior volume pago foi {nm} ({vtxt})."
    return (
        f"A unidade com maior volume pago foi {nm} ({vtxt}), "
        f"{fmt_pct(pct_total, decimals=1)} do total do ranking exibido."
    )


def insight_top_funcao(
    label: str | None,
    vl_pago: float | None,
    pct_total: float | None,
) -> str:
    if not label or not str(label).strip():
        return "Função/subfunção líder não identificada."
    lb = str(label).strip()
    if vl_pago is None:
        return f"A área funcional que mais concentrou pagamento: {lb}."
    vtxt = fmt_brl_compact(vl_pago)
    if pct_total is None:
        return f"Maior volume pago por função/subfunção: {lb} ({vtxt})."
    return (
        f"Maior volume pago por função/subfunção: {lb} ({vtxt}), "
        f"{fmt_pct(pct_total, decimals=1)} do total do ranking."
    )
