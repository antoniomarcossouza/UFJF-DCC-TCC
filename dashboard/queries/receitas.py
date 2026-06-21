"""Queries SQL de receitas (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_receita_where,
    receita_acumulada_from_joins,
    receita_from_joins,
)


def _receita_where_so_natureza(filters: FilterState, params: list) -> str:
    """WHERE de receita apenas com filtro de natureza (sem ano/mês)."""
    flt = FilterState(sk_naturezas_receita=filters.sk_naturezas_receita)
    return build_receita_where(flt, params)


def kpis_execucao_receita(filters: FilterState) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        with snapshot as (
            select
                f.vl_previsao_inicial_comparativa,
                f.vl_previsao_atualizada,
                f.vl_arrecadada_ano,
                f.vl_a_realizar
            {receita_acumulada_from_joins()}
            where {where}
            qualify row_number() over (
                partition by f.sk_natureza_receita, t.nu_ano
                order by t.nu_mes desc
            ) = 1
        )
        select
            coalesce(sum(vl_previsao_inicial_comparativa), 0)
                as vl_previsao_inicial,
            coalesce(sum(vl_previsao_atualizada), 0) as vl_previsao_atualizada,
            coalesce(sum(vl_arrecadada_ano), 0) as vl_arrecadada_ano,
            coalesce(sum(vl_a_realizar), 0) as vl_a_realizar
        from snapshot
    """
    return sql, params


def serie_mensal_arrecadacao(filters: FilterState) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            t.nu_ano,
            t.nu_mes,
            t.sg_mes,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada,
            coalesce(sum(f.vl_previsto_mensal), 0) as vl_previsto
        {receita_from_joins()}
        where {where}
        group by t.nu_ano, t.nu_mes, t.sg_mes
        order by t.nu_ano, t.nu_mes
    """
    return sql, params


def previsto_realizado_por_natureza(filters: FilterState) -> tuple[str, list]:
    # Mesmo cuidado do KPI: usa só o snapshot acumulado mais recente de cada
    # (natureza, ano) antes de agregar, para não somar o year-to-date repetido.
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        with snapshot as (
            select
                nr.cd_natureza_receita,
                nr.ds_natureza_receita,
                f.vl_arrecadada_ano,
                f.vl_previsao_atualizada
            {receita_acumulada_from_joins()}
            where {where}
            qualify row_number() over (
                partition by f.sk_natureza_receita, t.nu_ano
                order by t.nu_mes desc
            ) = 1
        )
        select
            cd_natureza_receita,
            ds_natureza_receita,
            coalesce(sum(vl_arrecadada_ano), 0) as vl_arrecadada_ano,
            coalesce(sum(vl_previsao_atualizada), 0) as vl_previsao_atualizada,
            case
                when sum(vl_previsao_atualizada) > 0
                then 100.0 * sum(vl_arrecadada_ano)
                    / sum(vl_previsao_atualizada)
                else null
            end as pct_realizacao
        from snapshot
        group by cd_natureza_receita, ds_natureza_receita
        having sum(vl_previsao_atualizada) > 0
        order by pct_realizacao asc nulls last
    """
    return sql, params


def serie_anual_previsto_realizado(filters: FilterState) -> tuple[str, list]:
    """Soma mensal por ano: arrecadado e previsto (série histórica)."""
    params: list = []
    where_nat = _receita_where_so_natureza(filters, params)
    sql = f"""
        select
            t.nu_ano,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada,
            coalesce(sum(f.vl_previsto_mensal), 0) as vl_previsto
        {receita_from_joins()}
        where {where_nat}
        group by t.nu_ano
        order by t.nu_ano
    """
    return sql, params


def arrecadacao_por_origem(filters: FilterState) -> tuple[str, list]:
    """Agregação por prefixo de 3 dígitos do código MCASP (mapear no shell)."""
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            substr(nr.cd_natureza_receita, 1, 3) as cd_prefixo3,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
        {receita_from_joins()}
        where {where}
        group by substr(nr.cd_natureza_receita, 1, 3)
        order by vl_arrecadada desc
    """
    return sql, params
