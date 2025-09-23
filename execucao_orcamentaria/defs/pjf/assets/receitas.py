import re
from pathlib import Path

import dagster as dg
import pandas as pd
import requests
from dagster.components import definitions
from dagster_duckdb import DuckDBResource

from execucao_orcamentaria.defs.filesystem.resources import LocalFSResource
from execucao_orcamentaria.defs.pjf.partitions import (
    year_month_partition,
    year_partition,
)
from execucao_orcamentaria.utils.duckdb import write_df_to_duckdb


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


@dg.asset(
    partitions_def=year_partition,
    kinds={"excel", "pandas", "duckdb"},
    group_name="pjf",
)
def receita_mensal_prevista(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """
    Baixa o XLS de receita mensal prevista,
    lê com pandas e salva em uma tabela DuckDB.
    Suporta backfill anual baseado em partições.
    """
    year = int(context.partition_key)

    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/previsao/arquivos/xls/{year}.xls"
    content = fetch_xls(url)

    filepath = fs.save_bytes(
        content=content,
        directory="pjf_receita_mensal_prevista",
        filename=url.split("/")[-1],
    )

    df = read_receita_prevista(filepath)

    write_df_to_duckdb(
        duckdb=duckdb,
        _df=df,
        schema="stg",
        table="pjf_receita_mensal_prevista",
    )

    return dg.MaterializeResult()


@dg.asset(
    partitions_def=year_month_partition,
    kinds={"excel", "pandas", "duckdb"},
    group_name="pjf",
)
def receita_mensal_comparativa(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    year_month = context.partition_key

    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/comparativo/arquivos/xls/{year_month}.xls"
    content = fetch_xls(url)

    filepath = fs.save_bytes(
        content=content,
        directory="pjf_receita_mensal_comparativa",
        filename=url.split("/")[-1],
    )
    if int(filepath.stem) < 2505:
        df = read_receita_comparativa_pre2505(filepath)
    if int(filepath.stem) >= 2505:
        df = read_receita_comparativa_2505(filepath)

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
        assets=[receita_mensal_prevista, receita_mensal_comparativa]
    )
