import pandas as pd
from dagster_duckdb import DuckDBResource


def _quote_ident(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def write_df_to_duckdb(
    duckdb: DuckDBResource, _df: pd.DataFrame, schema: str, table: str
):
    """
    Insere um DataFrame pandas no DuckDB,
    criando schema/tabela se não existirem.
    """
    q_schema = _quote_ident(schema)
    q_table = _quote_ident(table)
    qualified_table = f"{q_schema}.{q_table}"

    with duckdb.get_connection() as conn:
        conn.execute(f"create schema if not exists {q_schema}")
        conn.execute(f"""
            create table if not exists {qualified_table} as
            select * from _df limit 0
            """)

        existing_cols = {
            row[0]
            for row in conn.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = ? and table_name = ?
                """,
                [schema, table],
            ).fetchall()
        }

        df_cols = set(_df.columns)
        new_cols = [col for col in _df.columns if col not in existing_cols]
        missing_cols = [col for col in existing_cols if col not in df_cols]

        for col in new_cols:
            conn.execute(
                "alter table "
                f"{qualified_table} "
                "add column "
                f"{_quote_ident(str(col))} varchar"
            )

        for col in missing_cols:
            _df[col] = None

        insert_cols = ", ".join(_quote_ident(str(col)) for col in _df.columns)
        conn.execute(
            "insert into "
            f"{qualified_table} ({insert_cols}) "
            f"select {insert_cols} from _df"
        )


def overwrite_partition_in_duckdb(
    duckdb: DuckDBResource,
    _df: pd.DataFrame,
    schema: str,
    table: str,
    partition_col: str,
    partition_value: str | int,
):
    """
    Sobrescreve 1 particao (delete + insert), mantendo idempotencia.
    """
    q_schema = _quote_ident(schema)
    q_table = _quote_ident(table)
    q_partition_col = _quote_ident(partition_col)
    qualified_table = f"{q_schema}.{q_table}"

    with duckdb.get_connection() as conn:
        conn.execute(f"create schema if not exists {q_schema}")
        conn.execute(f"""
            create table if not exists {qualified_table} as
            select * from _df limit 0
            """)

        conn.execute(
            f"delete from {qualified_table} where {q_partition_col} = ?",
            [partition_value],
        )

    write_df_to_duckdb(duckdb=duckdb, _df=_df, schema=schema, table=table)
