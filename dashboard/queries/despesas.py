"""Queries SQL de despesas (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    despesa_from_joins,
)


def kpis_despesa(filters: FilterState) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado,
            coalesce(sum(f.vl_liquidado_mes), 0) as vl_liquidado,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago
        {despesa_from_joins()}
        where {where}
    """
    return sql, params


def eficiencia_execucao(filters: FilterState) -> tuple[str, list]:
    """Percentuais liquidado/pago sobre empenhado (derivados dos fatos)."""
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado,
            coalesce(sum(f.vl_liquidado_mes), 0) as vl_liquidado,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago,
            case when sum(f.vl_empenhado_mes) > 0
                then 100.0 * sum(f.vl_liquidado_mes) / sum(f.vl_empenhado_mes)
                else null end as pct_liquidado_sobre_empenhado,
            case when sum(f.vl_empenhado_mes) > 0
                then 100.0 * sum(f.vl_pago_mes) / sum(f.vl_empenhado_mes)
                else null end as pct_pago_sobre_empenhado,
            case when sum(f.vl_liquidado_mes) > 0
                then 100.0 * sum(f.vl_pago_mes) / sum(f.vl_liquidado_mes)
                else null end as pct_pago_sobre_liquidado
        {despesa_from_joins()}
        where {where}
    """
    return sql, params


def serie_mensal_despesa(filters: FilterState) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            t.nu_ano,
            t.nu_mes,
            t.sg_mes,
            coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado,
            coalesce(sum(f.vl_liquidado_mes), 0) as vl_liquidado,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago
        {despesa_from_joins()}
        where {where}
        group by t.nu_ano, t.nu_mes, t.sg_mes
        order by t.nu_ano, t.nu_mes
    """
    return sql, params


def ranking_naturezas_despesa(
    filters: FilterState, top_n: int = 20
) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        with base as (
            select
                nd.cd_natureza_despesa,
                nd.ds_natureza_despesa,
                coalesce(sum(f.vl_pago_mes), 0) as vl_pago
            {despesa_from_joins()}
            where {where}
            group by nd.cd_natureza_despesa, nd.ds_natureza_despesa
        ),
        total as (select sum(vl_pago) as geral from base)
        select
            b.cd_natureza_despesa,
            b.ds_natureza_despesa,
            b.vl_pago,
            case when t.geral > 0
                then 100.0 * b.vl_pago / t.geral
                else null end as pct_participacao
        from base b cross join total t
        order by b.vl_pago desc
        limit {top_n}
    """
    return sql, params


def aceleracao_gastos(filters: FilterState) -> tuple[str, list]:
    """Pagamentos por mês para detectar concentração no fim do período."""
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            t.nu_ano,
            t.nu_mes,
            t.sg_mes,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago,
            coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado
        {despesa_from_joins()}
        where {where}
        group by t.nu_ano, t.nu_mes, t.sg_mes
        order by t.nu_ano, t.nu_mes
    """
    return sql, params
