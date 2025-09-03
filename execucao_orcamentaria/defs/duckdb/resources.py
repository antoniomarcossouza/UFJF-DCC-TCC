from pathlib import Path

import dagster as dg
from dagster.components import definitions
from dagster_duckdb import DuckDBResource


@definitions
def defs():
    return dg.Definitions(
        resources={
            "duckdb": DuckDBResource(
                database=str(
                    (Path.cwd() / "data").resolve()
                    / "execucao_orcamentaria.duckdb"
                ),
            )
        },
    )
