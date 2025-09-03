import re
from datetime import datetime
from pathlib import Path

import dagster as dg
import pandas as pd
import requests
from dagster.components import definitions
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


def fetch_xls(url: str) -> bytes:
    """Baixa XLS bruto de uma URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def read_receita_comparativa_pre2505(filepath: Path):
    def rename_cols_pre2505(col: str) -> str:
        col = col.strip()
        col = col.replace("\n", " ")
        col = re.sub(r"\s+", " ", col)
        col = col.lower()

        if "receita total" in col:
            return "Código"

        if "unnamed: 1" in col:
            return "Descrição"

        if "previsão inicial" in col:
            return "Previsão Inicial"

        if "previsão atualizada" in col:
            return "Previsão Atualizada"

        if "arrecadada" in col and " em" in col:
            return "Arrecadada Mês"

        if "arrecadada" in col and " até" in col:
            return "Arrecadada Ano"

        if "a realizar" in col:
            return "A Realizar"

        return col

    df = pd.read_excel(
        filepath,
        skiprows=1,
    )
    df.columns = [rename_cols_pre2505(c) for c in df.columns]
    df["Natureza"] = (
        df["Código"].astype(str) + " - " + df["Descrição"].astype(str)
    )
    df = df.drop(columns=["Código", "Descrição"])
    df["nm_arquivo"] = filepath.name
    df["dt_atualizacao"] = pd.Timestamp.utcnow()
    return df


def read_receita_comparativa_2505(filepath: Path):
    df = pd.read_excel(filepath, skiprows=5)
    df.columns = [
        "Natureza",
        "Fonte TCE",
        "Previsão Inicial",
        "Previsão Atualizada",
        "Arrecadada Mês",
        "Arrecadada Ano",
        "A Realizar",
    ]
    df["nm_arquivo"] = filepath.name
    df["dt_atualizacao"] = pd.Timestamp.utcnow()
    return df


def read_receita_prevista(filepath: Path):
    df = pd.read_excel(filepath, skiprows=2)
    df["NATUREZA DE RECEITA"] = (
        df["NATUREZA DE RECEITA"].astype(str)
        + " - "
        + df["Unnamed: 1"].astype(str)
    )
    df = df.drop(columns=["Unnamed: 1"])
    df["nm_arquivo"] = filepath.name
    df["dt_atualizacao"] = pd.Timestamp.utcnow()

    df.columns = [c.title() for c in df.columns]

    return df


class LocalFSResource(dg.ConfigurableResource):
    base_path: str

    def save_bytes(
        self,
        content: bytes,
        directory: str,
        filename: str,
    ) -> None:
        """Salva bytes no filesystem local."""
        filepath = Path(self.base_path) / directory / filename
        filepath.parent.mkdir(exist_ok=True, parents=True)

        with filepath.open("wb") as f:
            f.write(content)

    def glob(self, directory: str, pattern: str = "*"):
        """
        Retorna uma lista de Paths dentro de
        `directory` que batem com `pattern`.
        """
        dir_path = Path(self.base_path) / directory
        if not dir_path.exists():
            return []
        return list(dir_path.glob(pattern))


receita_mensal_prevista_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%y",
    end_offset=1,
)

receita_mensal_comparativa_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 * *",
    fmt="%y%m",
)


@dg.asset(
    partitions_def=receita_mensal_prevista_partition,
    kinds={"python", "excel"},
    group_name="pjf",
)
def receita_mensal_prevista(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
) -> dg.MaterializeResult:
    """
    Baixa o XLS de receita mensal prevista,
    lê com pandas e salva em uma tabela DuckDB.
    Suporta backfill anual baseado em partições.
    """
    year = int(context.partition_key)

    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/previsao/arquivos/xls/{year}.xls"
    content = fetch_xls(url)

    fs.save_bytes(
        content=content,
        directory="pjf_receita_mensal_prevista",
        filename=url.split("/")[-1],
    )

    return dg.MaterializeResult()


@dg.asset(
    kinds={"pandas", "duckdb"},
    group_name="pjf",
    deps=[receita_mensal_prevista],
)
def stg_receita_mensal_prevista(
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    receita_prevista = [
        f for f in fs.glob("pjf_receita_mensal_prevista", "*.xls")
    ]

    df = pd.concat([read_receita_prevista(r) for r in receita_prevista])

    write_df_to_duckdb(
        duckdb=duckdb,
        _df=df,
        schema="stg",
        table="pjf_receita_mensal_prevista",
    )

    return dg.MaterializeResult()


@dg.asset(
    partitions_def=receita_mensal_comparativa_partition,
    kinds={"python", "excel"},
    group_name="pjf",
)
def receita_mensal_comparativa(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
) -> dg.MaterializeResult:
    year_month = context.partition_key

    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/comparativo/arquivos/xls/{year_month}.xls"
    content = fetch_xls(url)

    fs.save_bytes(
        content=content,
        directory="pjf_receita_mensal_comparativa",
        filename=url.split("/")[-1],
    )

    return dg.MaterializeResult()


@dg.asset(
    kinds={"pandas", "duckdb"},
    group_name="pjf",
    deps=[receita_mensal_comparativa],
)
def stg_receita_mensal_comparativa(
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    receita_mensal_pre2505 = [
        f
        for f in fs.glob("pjf_receita_mensal_comparativa", "*.xls")
        if int(f.name.split(".")[0]) < 2505
    ]

    receita_mensal_2505 = [
        f
        for f in fs.glob("pjf_receita_mensal_comparativa", "*.xls")
        if int(f.name.split(".")[0]) >= 2505
    ]

    df = pd.concat(
        [read_receita_comparativa_pre2505(r) for r in receita_mensal_pre2505]
        + [
            read_receita_comparativa_2505(r)
            for r in receita_mensal_2505
        ]
    )

    write_df_to_duckdb(
        duckdb=duckdb,
        _df=df,
        schema="stg",
        table="pjf_receita_mensal_comparativa",
    )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(
        assets=[
            receita_mensal_prevista,
            receita_mensal_comparativa,
            stg_receita_mensal_prevista,
            stg_receita_mensal_comparativa,
        ],
        resources={
            "fs": LocalFSResource(
                base_path=str((Path.cwd() / "data").resolve())
            )
        },
    )
