"""Queries de análise fiscal consolidada (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    build_receita_where,
    despesa_from_joins,
    receita_from_joins,
)


def kpis_fiscal(filters: FilterState) -> tuple[str, list]:
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
        select
            r.vl_arrecadada,
            d.vl_pago,
            r.vl_arrecadada - d.vl_pago as vl_saldo,
            case when r.vl_arrecadada > 0
                then 100.0 * d.vl_pago / r.vl_arrecadada else null end
                as pct_despesa_sobre_receita
        from rec r cross join desp d
    """
    return sql, params_r + params_d


def serie_mensal_fiscal(filters: FilterState) -> tuple[str, list]:
    params_r: list = []
    params_d: list = []
    where_r = build_receita_where(filters, params_r)
    where_d = build_despesa_where(filters, params_d)
    sql = f"""
        with rec as (
            select t.nu_ano, t.nu_mes, t.sg_mes,
                coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
            {receita_from_joins()}
            where {where_r}
            group by t.nu_ano, t.nu_mes, t.sg_mes
        ),
        desp as (
            select t.nu_ano, t.nu_mes, t.sg_mes,
                coalesce(sum(f.vl_pago_mes), 0) as vl_pago
            {despesa_from_joins()}
            where {where_d}
            group by t.nu_ano, t.nu_mes, t.sg_mes
        )
        select
            coalesce(r.nu_ano, d.nu_ano) as nu_ano,
            coalesce(r.nu_mes, d.nu_mes) as nu_mes,
            coalesce(r.sg_mes, d.sg_mes) as sg_mes,
            coalesce(r.vl_arrecadada, 0) as vl_arrecadada,
            coalesce(d.vl_pago, 0) as vl_pago,
            coalesce(r.vl_arrecadada, 0) - coalesce(d.vl_pago, 0) as vl_saldo_mensal
        from rec r
        full outer join desp d
            on r.nu_ano = d.nu_ano and r.nu_mes = d.nu_mes
        order by nu_ano, nu_mes
    """
    return sql, params_r + params_d


def saldo_mensal_ultimo_mes(filters: FilterState) -> tuple[str, list]:
    """Compara arrecadação vs pagamento no último mês com dados (pergunta 6)."""
    params_r: list = []
    params_d: list = []
    where_r = build_receita_where(filters, params_r)
    where_d = build_despesa_where(filters, params_d)
    sql = f"""
        with ultimo as (
            select max(make_date(t.nu_ano, t.nu_mes, 1)) as ref
            from dwh.dim_tempo t
            where exists (
                select 1 from dwh.fct_receita f
                where f.sk_tempo_referencia = t.sk_tempo
            ) or exists (
                select 1 from dwh.fct_despesa f
                where f.sk_tempo_empenho = t.sk_tempo
            )
        ),
        rec as (
            select coalesce(sum(f.vl_arrecadada_mes), 0) as vl
            {receita_from_joins()}
            cross join ultimo u
            where {where_r}
                and make_date(t.nu_ano, t.nu_mes, 1) = u.ref
        ),
        desp as (
            select coalesce(sum(f.vl_pago_mes), 0) as vl
            {despesa_from_joins()}
            cross join ultimo u
            where {where_d}
                and make_date(t.nu_ano, t.nu_mes, 1) = u.ref
        )
        select
            (select ref from ultimo) as dt_ref,
            r.vl as vl_arrecadada_mes,
            d.vl as vl_pago_mes,
            r.vl - d.vl as vl_saldo_mensal
        from rec r cross join desp d
    """
    return sql, params_r + params_d
