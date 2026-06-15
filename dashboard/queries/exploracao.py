"""Query de exploração detalhada (functional core)."""

from __future__ import annotations

from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    build_receita_where,
)


def detalhe_receita(
    filters: FilterState, limit: int = 500
) -> tuple[str, list]:
    params: list = []
    where = build_receita_where(filters, params)
    sql = f"""
        select
            t.nu_ano,
            t.nu_mes,
            nr.cd_natureza_receita,
            nr.ds_natureza_receita,
            f.vl_previsto_mensal,
            f.vl_arrecadada_mes
        from dwh.fct_receita f
        join dwh.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
        join dwh.dim_natureza_receita nr
            on f.sk_natureza_receita = nr.sk_natureza_receita
        where {where}
        order by t.nu_ano desc, t.nu_mes desc, nr.cd_natureza_receita
        limit {limit}
    """
    return sql, params


def detalhe_despesa(
    filters: FilterState, limit: int = 500
) -> tuple[str, list]:
    params: list = []
    where = build_despesa_where(filters, params)
    sql = f"""
        select
            t.nu_ano,
            t.nu_mes,
            ua.nm_unidade_administrativa,
            fo.nm_fornecedor,
            substr(fp.cd_funcional_pragmatica, 1, 2) as cd_funcao,
            nd.cd_natureza_despesa,
            nd.ds_natureza_despesa,
            fr.cd_fonte_recurso,
            f.vl_empenhado_mes,
            f.vl_liquidado_mes,
            f.vl_pago_mes
        from dwh.fct_despesa f
        join dwh.dim_tempo t on f.sk_tempo_empenho = t.sk_tempo
        left join dwh.dim_unidade_administrativa ua
            on f.sk_unidade_administrativa = ua.sk_unidade_administrativa
        left join dwh.dim_fornecedor fo on f.sk_fornecedor = fo.sk_fornecedor
        left join dwh.dim_funcional_pragmatica fp
            on f.sk_funcional_pragmatica = fp.sk_funcional_pragmatica
        left join dwh.dim_natureza_despesa nd
            on f.sk_natureza_despesa = nd.sk_natureza_despesa
        left join dwh.dim_fonte_recurso fr
            on f.sk_fonte_recurso = fr.sk_fonte_recurso
        where {where}
        order by t.nu_ano desc, t.nu_mes desc, f.vl_pago_mes desc
        limit {limit}
    """
    return sql, params
