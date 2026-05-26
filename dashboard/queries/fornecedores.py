"""Queries de fornecedores (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    despesa_from_joins,
)


def ranking_fornecedores(
    filters: FilterState, top_n: int = 25
) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        with base as (
            select
                fo.sk_fornecedor,
                fo.nm_fornecedor,
                fo.cd_cpf_cnpj,
                coalesce(sum(f.vl_pago_mes), 0) as vl_pago,
                count(distinct f.sk_empenho) as qtd_empenhos
            {despesa_from_joins()}
            where {where}
            group by fo.sk_fornecedor, fo.nm_fornecedor, fo.cd_cpf_cnpj
        ),
        total as (select sum(vl_pago) as geral from base),
        ranked as (
            select
                b.*,
                case when t.geral > 0
                    then 100.0 * b.vl_pago / t.geral else null end as pct_total,
                sum(b.vl_pago) over (order by b.vl_pago desc) as vl_acumulado,
                t.geral
            from base b cross join total t
        )
        select
            sk_fornecedor,
            nm_fornecedor,
            cd_cpf_cnpj,
            vl_pago,
            qtd_empenhos,
            pct_total,
            case when geral > 0
                then 100.0 * vl_acumulado / geral else null end as pct_acumulado
        from ranked
        order by vl_pago desc
        limit {top_n}
    """
    return sql, params


def evolucao_fornecedor(
    filters: FilterState,
    sk_fornecedor: str,
) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    params.append(sk_fornecedor)
    sql = f"""
        select
            t.nu_ano,
            t.nu_mes,
            t.sg_mes,
            coalesce(sum(f.vl_pago_mes), 0) as vl_pago,
            count(distinct f.sk_empenho) as qtd_empenhos
        {despesa_from_joins()}
        where ({where}) and f.sk_fornecedor = ?
        group by t.nu_ano, t.nu_mes, t.sg_mes
        order by t.nu_ano, t.nu_mes
    """
    return sql, params
