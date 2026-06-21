"""Queries por unidade administrativa (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    despesa_from_joins,
)


def execucao_por_unidade(
    filters: FilterState
) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            ua.nm_unidade_administrativa,
            coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado,
            coalesce(sum(f.vl_liquidado_mes), 0) as vl_liquidado,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago
        {despesa_from_joins()}
        where {where}
        group by ua.nm_unidade_administrativa
        order by vl_pago desc
    """
    return sql, params


def execucao_por_funcao(
    filters: FilterState
) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            substr(fp.cd_funcional_pragmatica, 1, 2) as cd_funcao,
            substr(fp.cd_funcional_pragmatica, 4, 3) as cd_subfuncao,
            max(fp.ds_funcional_pragmatica) as ds_acao_exemplo,
            coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado,
            coalesce(sum(f.vl_liquidado_mes), 0) as vl_liquidado,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago
        {despesa_from_joins()}
        where {where}
        group by 1, 2
        order by vl_pago desc
    """
    return sql, params
