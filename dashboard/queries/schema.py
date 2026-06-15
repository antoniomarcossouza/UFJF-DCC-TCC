"""Introspecção e contrato de colunas do warehouse (functional core)."""

from __future__ import annotations

SCHEMA = "dwh"


def data_coverage_sql() -> tuple[str, list]:
    return (
        f"""
        select
            'fct_receita' as fato,
            min(t.dt_dia) as dt_min,
            max(t.dt_dia) as dt_max,
            count(distinct t.nu_ano || '-' || t.nu_mes) as qtd_meses
        from {SCHEMA}.fct_receita f
        join {SCHEMA}.dim_tempo t on f.sk_tempo_referencia = t.sk_tempo
        union all
        select
            'fct_despesa',
            min(t.dt_dia),
            max(t.dt_dia),
            count(distinct t.nu_ano || '-' || t.nu_mes)
        from {SCHEMA}.fct_despesa f
        join {SCHEMA}.dim_tempo t on f.sk_tempo_empenho = t.sk_tempo
        """,
        [],
    )
