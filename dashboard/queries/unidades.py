"""Queries por unidade administrativa (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    despesa_from_joins,
    funcional_cd_funcao_sql,
    funcional_cd_subfuncao_sql,
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
    cd_funcao = funcional_cd_funcao_sql("fp")
    cd_subfuncao = funcional_cd_subfuncao_sql("fp")
    sql = f"""
        with execucao as (
            select
                {cd_funcao} as cd_funcao,
                {cd_subfuncao} as cd_subfuncao,
                coalesce(sum(f.vl_empenhado_mes), 0) as vl_empenhado,
                coalesce(sum(f.vl_liquidado_mes), 0) as vl_liquidado,
                coalesce(sum(f.vl_pago_mes), 0) as vl_pago
            {despesa_from_joins()}
            where {where}
            group by 1, 2
        )
        select
            e.cd_funcao,
            coalesce(fn.ds_funcao, 'Função ' || e.cd_funcao) as ds_funcao,
            e.cd_subfuncao,
            coalesce(
                sf.ds_subfuncao,
                'Subfunção ' || e.cd_subfuncao
            ) as ds_subfuncao,
            e.vl_empenhado,
            e.vl_liquidado,
            e.vl_pago
        from execucao e
        left join dwh.dim_funcao fn on e.cd_funcao = fn.cd_funcao
        left join dwh.dim_subfuncao sf on e.cd_subfuncao = sf.cd_subfuncao
        order by e.vl_pago desc
    """
    return sql, params
