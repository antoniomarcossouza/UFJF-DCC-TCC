"""Queries para indicadores per capita (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    build_receita_where,
    despesa_from_joins,
    receita_from_joins,
)


def totais_per_capita(filters: FilterState) -> tuple[str, list]:
    params_r: list = []
    params_d: list = []
    where_r = build_receita_where(filters, params_r)
    where_d = build_despesa_where(filters, params_d)
    sql = f"""
        with rec as (
            select coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
            {receita_from_joins()}
            where {where_r}
        ),
        desp as (
            select coalesce(sum(f.vl_pago_mes), 0) as vl_pago
            {despesa_from_joins()}
            where {where_d}
        )
        select r.vl_arrecadada, d.vl_pago
        from rec r cross join desp d
    """
    return sql, params_r + params_d


def gasto_por_funcao(filters: FilterState, cd_funcao: str) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    params.append(cd_funcao)
    sql = f"""
        select coalesce(sum(f.vl_pago_mes), 0) as vl_pago
        {despesa_from_joins()}
        where ({where})
            and substr(fp.cd_funcional_pragmatica, 1, 2) = ?
    """
    return sql, params
