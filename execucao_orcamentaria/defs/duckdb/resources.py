import dagster as dg
from dagster.components import definitions
from dagster_duckdb import DuckDBResource


@definitions
def defs():
    return dg.Definitions(
        resources={
            "duckdb": DuckDBResource(
                database="/home/antonio/projects/ufjf/ufjf-tcc/data/execucao_orcamentaria.duckdb",
            )
        },
    )
