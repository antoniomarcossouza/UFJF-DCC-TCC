"""Queries SQL de receitas (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_receita_where,
    receita_acumulada_from_joins,
    receita_from_joins,
)


def _receita_where_so_natureza(
    filters: FilterState, params: list
) -> str:
    """WHERE de receita apenas com filtro de natureza (sem ano/mês)."""
    flt = FilterState(sk_naturezas_receita=filters.sk_naturezas_receita)
    return build_receita_where(flt, params)


def kpis_execucao_receita(filters: FilterState) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            coalesce(sum(f.vl_previsao_inicial_comparativa), 0)
                as vl_previsao_inicial,
            coalesce(sum(f.vl_previsao_atualizada), 0) as vl_previsao_atualizada,
            coalesce(sum(f.vl_arrecadada_ano), 0) as vl_arrecadada_ano,
            coalesce(sum(f.vl_a_realizar), 0) as vl_a_realizar
        {receita_acumulada_from_joins()}
        where {where}
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


def heatmap_sazonalidade(
    filters: FilterState, top_n: int = 15
) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        with filtered as (
            select
                t.nu_mes,
                t.sg_mes,
                nr.cd_natureza_receita,
                nr.ds_natureza_receita,
                f.vl_arrecadada_mes
            {receita_from_joins()}
            where {where}
        ),
        ranked as (
            select
                cd_natureza_receita,
                ds_natureza_receita,
                sum(vl_arrecadada_mes) as total
            from filtered
            group by cd_natureza_receita, ds_natureza_receita
            order by total desc
            limit {top_n}
        )
        select
            f.nu_mes,
            f.sg_mes,
            f.cd_natureza_receita,
            f.ds_natureza_receita,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
        from filtered f
        inner join ranked r
            on f.cd_natureza_receita = r.cd_natureza_receita
        group by f.nu_mes, f.sg_mes, f.cd_natureza_receita, f.ds_natureza_receita
        order by f.nu_mes, f.cd_natureza_receita
    """
    return sql, params


def ranking_naturezas(
    filters: FilterState, top_n: int = 20
) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        with base as (
            select
                nr.cd_natureza_receita,
                nr.ds_natureza_receita,
                coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
            {receita_from_joins()}
            where {where}
            group by nr.cd_natureza_receita, nr.ds_natureza_receita
        ),
        total as (select sum(vl_arrecadada) as geral from base)
        select
            b.cd_natureza_receita,
            b.ds_natureza_receita,
            b.vl_arrecadada,
            case when t.geral > 0
                then 100.0 * b.vl_arrecadada / t.geral else null end as pct_participacao
        from base b cross join total t
        order by b.vl_arrecadada desc
        limit {top_n}
    """
    return sql, params


def receitas_abaixo_previsto(
    filters: FilterState, limite_pct: float = 80.0
) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            nr.cd_natureza_receita,
            nr.ds_natureza_receita,
            coalesce(sum(f.vl_arrecadada_ano), 0) as vl_arrecadada_ano,
            coalesce(sum(f.vl_previsao_atualizada), 0) as vl_previsao_atualizada,
            case
                when sum(f.vl_previsao_atualizada) > 0
                then 100.0 * sum(f.vl_arrecadada_ano)
                    / sum(f.vl_previsao_atualizada)
                else null
            end as pct_realizacao
        {receita_acumulada_from_joins()}
        where {where}
        group by nr.cd_natureza_receita, nr.ds_natureza_receita
        having sum(f.vl_previsao_atualizada) > 0
            and (
                100.0 * sum(f.vl_arrecadada_ano) / sum(f.vl_previsao_atualizada)
            ) < {limite_pct}
        order by pct_realizacao asc nulls last
    """
    return sql, params


def deducoes_receita(filters: FilterState) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            coalesce(sum(case when nr.cd_natureza_receita like '9%'
                then f.vl_arrecadada_mes else 0 end), 0) as vl_deducoes_categoria9,
            coalesce(sum(case when f.vl_arrecadada_mes < 0
                then f.vl_arrecadada_mes else 0 end), 0) as vl_valores_negativos,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_bruto,
            coalesce(sum(case when f.vl_arrecadada_mes > 0
                then f.vl_arrecadada_mes else 0 end), 0) as vl_positivo
        {receita_from_joins()}
        where {where}
    """
    return sql, params


def kpis_yoy_mesma_janela(filters: FilterState) -> tuple[str, list]:
    """YoY ignorando filtro de ano: ano corrente = max(nu_ano) na base.

    Janela de meses: se houver filtro de mês, usa esses meses nos dois anos;
    senão, jan até último mês com dado no ano corrente.
    """
    params: list = []
    where_nat = _receita_where_so_natureza(filters, params)
    if filters.meses:
        placeholders = ", ".join("?" for _ in filters.meses)
        mes_clause = f"t.nu_mes in ({placeholders})"
        params.extend(int(m) for m in filters.meses)
    else:
        mes_clause = "t.nu_mes <= ml.nu_mes_max"
    sql = f"""
        with ano_ref as (
            select max(t.nu_ano) as ano_atual
            from dwh.fct_receita f
            join dwh.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
        ),
        mes_lim as (
            select max(t.nu_mes) as nu_mes_max
            from dwh.fct_receita f
            join dwh.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
            cross join ano_ref ar
            where t.nu_ano = ar.ano_atual
        )
        select
            ar.ano_atual,
            ar.ano_atual - 1 as ano_anterior,
            ml.nu_mes_max as mes_limite_corrente,
            coalesce(
                sum(
                    case when t.nu_ano = ar.ano_atual
                    then f.vl_arrecadada_mes else 0 end
                ),
                0
            ) as vl_atual,
            coalesce(
                sum(
                    case when t.nu_ano = ar.ano_atual - 1
                    then f.vl_arrecadada_mes else 0 end
                ),
                0
            ) as vl_anterior
        from dwh.fct_receita f
        join dwh.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
        join dwh.dim_natureza_receita nr
            on f.sk_natureza_receita = nr.sk_natureza_receita
        cross join ano_ref ar
        cross join mes_lim ml
        where (t.nu_ano = ar.ano_atual or t.nu_ano = ar.ano_atual - 1)
            and ({where_nat})
            and ({mes_clause})
        group by ar.ano_atual, ml.nu_mes_max
    """
    return sql, params


def serie_anual_arrecadacao(filters: FilterState) -> tuple[str, list]:
    """Total arrecadado por ano (série histórica); respeita só natureza."""
    params: list = []
    where_nat = _receita_where_so_natureza(filters, params)
    sql = f"""
        select
            t.nu_ano,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
        {receita_from_joins()}
        where {where_nat}
        group by t.nu_ano
        order by t.nu_ano
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


def principal_fonte(filters: FilterState) -> tuple[str, list]:
    """Maior prefixo MCASP (3 dígitos) por valor arrecadado no recorte."""
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        with agg as (
            select
                substr(nr.cd_natureza_receita, 1, 3) as cd_prefixo3,
                coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
            {receita_from_joins()}
            where {where}
            group by substr(nr.cd_natureza_receita, 1, 3)
        ),
        tot as (select sum(vl_arrecadada) as geral from agg)
        select
            a.cd_prefixo3,
            a.vl_arrecadada,
            case when t.geral > 0
                then 100.0 * a.vl_arrecadada / t.geral
                else null end as pct_participacao
        from agg a
        cross join tot t
        order by a.vl_arrecadada desc
        limit 1
    """
    return sql, params


def detalhe_deducoes(
    filters: FilterState, top_n: int = 30
) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            nr.cd_natureza_receita,
            nr.ds_natureza_receita,
            coalesce(sum(f.vl_arrecadada_mes), 0) as vl_arrecadada
        {receita_from_joins()}
        where ({where}) and (
            nr.cd_natureza_receita like '9%'
            or f.vl_arrecadada_mes < 0
        )
        group by nr.cd_natureza_receita, nr.ds_natureza_receita
        order by vl_arrecadada asc
        limit {top_n}
    """
    return sql, params
