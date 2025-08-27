from datetime import datetime

import dagster as dg
import pandas as pd
import requests
from dagster.components import definitions
from dagster_duckdb import DuckDBResource

receita_mensal_prevista_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%y",
)


@dg.asset(
    partitions_def=receita_mensal_prevista_partition,
    kinds={"python", "pandas", "duckdb"},
    group_name="receitas",
    description="Receita Mensal Prevista",
)
def receita_mensal_prevista(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """
    Baixa o XLS de receita mensal prevista,
    lê com pandas e salva em uma tabela DuckDB.
    Suporta backfill anual baseado em partições.
    """
    ano = int(context.partition_key)
    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/previsao/arquivos/xls/{ano}.xls"

    response = requests.get(url)
    response.raise_for_status()

    df = pd.read_excel(response.content, skiprows=2)
    df["filename"] = url.split("/")[-1]
    df["imported_at"] = pd.Timestamp.utcnow()

    with duckdb.get_connection() as conn:
        conn.execute("create schema if not exists stg")
        conn.execute(
            """
            create table if not exists stg.pjf_receita_mensal_prevista as
            select * from df limit 0
            """
        )
        conn.execute(
            "insert into stg.pjf_receita_mensal_prevista select * from df"
        )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(assets=[receita_mensal_prevista])
