import pandas as pd
from dagster_duckdb import DuckDBResource


def write_df_to_duckdb(
    duckdb: DuckDBResource, _df: pd.DataFrame, schema: str, table: str
):
    """
    Insere um DataFrame pandas no DuckDB,
    criando schema/tabela se não existirem.
    """
    with duckdb.get_connection() as conn:
        conn.execute(f"create schema if not exists {schema}")
        conn.execute(f"""
            create table if not exists {schema}.{table} as
            select * from _df limit 0
            """)
        conn.execute(f"insert into {schema}.{table} select * from _df")
