"""Introspecção e contrato de colunas do warehouse (functional core)."""

from __future__ import annotations

SCHEMA = "dwh"

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "fct_despesa": [
        "sk_empenho",
        "sk_unidade_administrativa",
        "sk_tempo_empenho",
        "sk_tempo_liquidacao",
        "sk_fornecedor",
        "sk_funcional_pragmatica",
        "sk_natureza_despesa",
        "sk_fonte_recurso",
        "vl_empenhado_mes",
        "vl_liquidado_mes",
        "vl_pago_mes",
    ],
    "fct_receita": [
        "sk_natureza_receita",
        "sk_tempo_referencia",
        "vl_previsto_mensal",
        "vl_arrecadada_mes",
    ],
    "fct_receita_acumulada": [
        "sk_natureza_receita",
        "sk_tempo_referencia",
        "vl_previsao_inicial_comparativa",
        "vl_previsao_atualizada",
        "vl_arrecadada_ano",
        "vl_a_realizar",
    ],
    "dim_tempo": [
        "sk_tempo",
        "dt_dia",
        "nu_mes",
        "nu_ano",
        "nm_mes",
        "sg_mes",
    ],
    "dim_natureza_receita": [
        "sk_natureza_receita",
        "cd_natureza_receita",
        "ds_natureza_receita",
    ],
    "dim_natureza_despeza": [
        "sk_natureza_despesa",
        "cd_natureza_despesa",
        "ds_natureza_despesa",
    ],
    "dim_funcional_pragmatica": [
        "sk_funcional_pragmatica",
        "cd_funcional_pragmatica",
        "ds_funcional_pragmatica",
    ],
    "dim_fornecedor": [
        "sk_fornecedor",
        "cd_cpf_cnpj",
        "nm_fornecedor",
    ],
    "dim_unidade_administrativa": [
        "sk_unidade_administrativa",
        "nm_unidade_administrativa",
    ],
    "dim_fonte_recurso": [
        "sk_fonte_recurso",
        "cd_fonte_recurso",
        "ds_fonte_recurso",
    ],
}

FACT_TABLES = ("fct_despesa", "fct_receita", "fct_receita_acumulada")
DIM_TABLES = tuple(k for k in EXPECTED_COLUMNS if k.startswith("dim_"))


def introspect_tables_sql() -> tuple[str, list]:
    return (
        """
        select table_name, column_name, data_type
        from information_schema.columns
        where table_schema = ?
        order by table_name, ordinal_position
        """,
        [SCHEMA],
    )


def validate_contract(columns_df) -> list[str]:
    """Retorna lista de erros de contrato; vazia se OK."""
    errors: list[str] = []
    if columns_df.empty:
        return ["Nenhuma coluna encontrada no schema dwh."]

    actual: dict[str, set[str]] = {}
    for _, row in columns_df.iterrows():
        tbl = row["table_name"]
        actual.setdefault(tbl, set()).add(row["column_name"])

    for table, expected_cols in EXPECTED_COLUMNS.items():
        if table not in actual:
            errors.append(f"Tabela ausente: {table}")
            continue
        missing = set(expected_cols) - actual[table]
        if missing:
            errors.append(f"{table}: colunas ausentes {sorted(missing)}")

    return errors


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
