from pathlib import Path

import dagster as dg
import pandas as pd
import requests
import xlrd
from dagster.components import definitions
from dagster_duckdb import DuckDBResource

from execucao_orcamentaria.defs.filesystem.resources import LocalFSResource
from execucao_orcamentaria.defs.pjf.partitions import year_month_partition
from execucao_orcamentaria.utils.duckdb import write_df_to_duckdb


def fetch_xls(url: str) -> bytes:
    """Baixa XLS bruto de uma URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def find_header_despesa_mensal(
    arquivo: Path,
    chave: str = "Unidade Administrativa",
):
    wb = xlrd.open_workbook(arquivo, on_demand=True)
    sheet = wb.sheet_by_index(0)

    for i in range(sheet.nrows):
        if sheet.cell_value(i, 0) == chave:
            return i

    return None


def read_despesa_mensal(filepath: Path):
    df = pd.read_excel(
        io=filepath, skiprows=find_header_despesa_mensal(filepath)
    )
    df = df.astype(str)

    df["nm_arquivo"] = filepath.name
    df["dt_atualizacao"] = pd.Timestamp.utcnow()

    return df


@dg.asset(
    partitions_def=year_month_partition,
    kinds={"python", "excel"},
    group_name="pjf",
)
def despesa_mensal_consolidada(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
) -> dg.MaterializeResult:
    year_month = context.partition_key

    url = f"https://www.pjf.mg.gov.br/transparencia/despesas_publicas/mensal_consolidada/arquivos/xls/{year_month}.xls"
    content = fetch_xls(url)

    fs.save_bytes(
        content=content,
        directory="pjf_despesa_mensal_consolidada",
        filename=url.split("/")[-1],
    )

    return dg.MaterializeResult()


@dg.asset(
    kinds={"pandas", "duckdb"},
    group_name="pjf",
    deps=[despesa_mensal_consolidada],
)
def stg_despesa_mensal_consolidada(
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    despesa_mensal = [
        f for f in fs.glob("pjf_despesa_mensal_consolidada", "*.xls")
    ]

    df = pd.concat([read_despesa_mensal(r) for r in despesa_mensal])

    write_df_to_duckdb(
        duckdb=duckdb,
        _df=df,
        schema="stg",
        table="pjf_despesa_mensal_consolidada",
    )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(
        assets=[
            despesa_mensal_consolidada,
            stg_despesa_mensal_consolidada,
        ]
    )
