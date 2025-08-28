from datetime import datetime
from io import BytesIO

import dagster as dg
import pandas as pd
import requests
from dagster.components import definitions
from dagster_duckdb import DuckDBResource
from duckdb import DuckDBPyConnection


def fetch_xls(url: str) -> bytes:
    """Baixa XLS bruto de uma URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def parse_receita_excel(content: bytes, source_url: str) -> pd.DataFrame:
    """Transforma bytes de XLS em DataFrame com colunas extras."""
    df = pd.read_excel(BytesIO(content), skiprows=2)
    df["filename"] = source_url.split("/")[-1]
    df["imported_at"] = pd.Timestamp.utcnow()
    return df


def ensure_table_exists(
    conn: DuckDBPyConnection,
    df: pd.DataFrame,
    schema: str,
    table: str,
) -> None:
    """Cria a tabela staging se ainda não existir."""
    conn.execute(f"create schema if not exists {schema}")
    conn.execute(
        f"""
        create table if not exists {schema}.{table} as
        select * from df limit 0
        """
    )


def save_dataframe_to_duckdb(
    conn: DuckDBPyConnection,
    df: pd.DataFrame,
    schema: str,
    table: str,
) -> None:
    """Insere DataFrame em uma tabela existente."""
    conn.execute(f"insert into {schema}.{table} select * from df")


receita_mensal_prevista_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%y",
    end_offset=1,
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
    content = fetch_xls(url)
    df = parse_receita_excel(content, url)

    with duckdb.get_connection() as conn:
        context.log.info("Teste")

        ensure_table_exists(
            conn=conn,
            df=df,
            schema="stg",
            table="pjf_receita_mensal_prevista",
        )
        save_dataframe_to_duckdb(
            conn=conn,
            df=df,
            schema="stg",
            table="pjf_receita_mensal_prevista",
        )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(assets=[receita_mensal_prevista])
