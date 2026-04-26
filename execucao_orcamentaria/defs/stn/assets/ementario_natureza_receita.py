import re
import unicodedata
from pathlib import Path

import dagster as dg
import pandas as pd
import requests
from dagster.components import definitions
from dagster_duckdb import DuckDBResource

from execucao_orcamentaria.defs.filesystem.resources import LocalFSResource
from execucao_orcamentaria.defs.stn.partitions import year_partition_stn
from execucao_orcamentaria.utils.duckdb import (
    overwrite_partition_in_duckdb,
)

URL_ATUAL = (
    "https://www.gov.br/tesouronacional/pt-br/"
    "contabilidade-e-custos/federacao/"
    "ementario-da-classificacao-por-natureza-de-receita-tabela-de-codigos"
)
URL_ANTERIORES = (
    "https://www.gov.br/tesouronacional/pt-br/"
    "contabilidade-e-custos/federacao/"
    "edicoes-anteriores-ementario-da-receita-orcamentaria/"
    "edicao_anterior_ementario-da-classificacao-por-natureza-"
    "de-receita-tabela-de-codigos"
)


def page_url_for_year(year: int) -> str:
    if year >= 2024:
        return URL_ATUAL
    return URL_ANTERIORES


def extract_ementario_link(html: str, year: int) -> str:
    pattern = (
        r'<a [^>]*href="(?P<href>[^"]+)"[^>]*>\s*'
        r"Ement[aá]rio\s*-\s*Tabela\s+de\s+C[oó]digos\s*-\s*"
        rf"v[aá]lido\s+para\s+{year}\s*</a>"
    )
    match = re.search(pattern, html, flags=re.IGNORECASE)
    if not match:
        raise ValueError(
            "Link do ementario nao encontrado para ano "
            f"{year} no HTML da pagina."
        )
    return match.group("href")


def fetch_url(url: str) -> requests.Response:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response


def normalize_col_name(col: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(col))
    no_accents = "".join(
        ch for ch in normalized if not unicodedata.combining(ch)
    )
    no_accents = no_accents.strip().lower()
    return re.sub(r"\s+", " ", no_accents)


def read_ementario_natureza_receita(
    filepath: Path, year: int
) -> pd.DataFrame:
    xls = pd.ExcelFile(filepath)
    sheet_name = next(
        (
            sheet
            for sheet in xls.sheet_names
            if normalize_col_name(sheet).startswith("enr")
        ),
        xls.sheet_names[0],
    )

    # Layout padrao STN: header na segunda linha.
    header_idx = 1

    raw = pd.read_excel(
        filepath, sheet_name=sheet_name, header=None, dtype=str
    ).fillna("")
    for idx in range(len(raw.index)):
        row_values = [normalize_col_name(v) for v in raw.iloc[idx].tolist()]
        has_nr = any(v == "nr" for v in row_values)
        has_desc = any(
            "especific" in v or "descricao" in v or "ementa" in v
            for v in row_values
        )
        if has_nr and has_desc:
            header_idx = idx
            break

    df = pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        skiprows=header_idx,
        dtype=str,
    )
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    df["nu_ano_referencia"] = year
    df["nm_arquivo"] = filepath.name
    df["dt_atualizacao"] = pd.Timestamp.utcnow()
    return df


@dg.asset(
    partitions_def=year_partition_stn,
    kinds={"html", "excel", "pandas", "duckdb"},
    group_name="raw",
)
def stn_ementario_natureza_receita(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    year = int(context.partition_key)

    page_url = page_url_for_year(year)
    html = fetch_url(page_url).text
    file_url = extract_ementario_link(html, year)
    content = fetch_url(file_url).content

    filepath = fs.save_bytes(
        content=content,
        directory="stn_ementario_natureza_receita",
        filename=f"{year}.xlsx",
    )

    df = read_ementario_natureza_receita(filepath=filepath, year=year)

    overwrite_partition_in_duckdb(
        duckdb=duckdb,
        _df=df,
        schema="stg",
        table="stn_ementario_natureza_receita",
        partition_col="nu_ano_referencia",
        partition_value=year,
    )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(assets=[stn_ementario_natureza_receita])
