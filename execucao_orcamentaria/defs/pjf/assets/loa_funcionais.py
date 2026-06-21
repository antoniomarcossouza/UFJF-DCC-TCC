import zipfile
from io import BytesIO
from pathlib import Path

import dagster as dg
import pandas as pd
import requests
from dagster.components import definitions
from dagster_duckdb import DuckDBResource

from execucao_orcamentaria.defs.filesystem.resources import LocalFSResource
from execucao_orcamentaria.defs.pjf.partitions import year_partition_loa
from execucao_orcamentaria.parsers.loa_funcionais import extract_funcionais_zip
from execucao_orcamentaria.utils.duckdb import overwrite_partition_in_duckdb

USER_AGENT = (
    "Mozilla/5.0 (compatible; execucao-orcamentaria/1.0; +https://github.com/)"
)
LOA_DIR = "pjf_loa_funcionais"


def loa_funcionais_url(year: int) -> str:
    return (
        "https://www.pjf.mg.gov.br/transparencia/orcamento/loa/"
        f"{year}/arquivos/pdf/Funcionais.zip"
    )


def fetch_zip(url: str) -> bytes:
    response = requests.get(
        url,
        timeout=60,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    return response.content


def ensure_loa_zip(year: int, fs: LocalFSResource) -> tuple[bytes, Path]:
    """Baixa o zip se necessário e retorna bytes + caminho salvo."""
    zip_path = Path(fs.base_path) / LOA_DIR / f"{year}.zip"
    if zip_path.is_file():
        return zip_path.read_bytes(), zip_path

    zip_bytes = fetch_zip(loa_funcionais_url(year))
    zip_path = fs.save_bytes(
        content=zip_bytes,
        directory=LOA_DIR,
        filename=f"{year}.zip",
    )
    extract_dir = zip_path.parent / str(year)
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        zf.extractall(extract_dir)
    return zip_bytes, zip_path


def read_loa_funcionais(
    zip_bytes: bytes,
    year: int,
    nm_arquivo: str,
    extract_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    funcoes, subfuncoes = extract_funcionais_zip(
        zip_bytes,
        extract_dir=extract_dir,
    )
    now = pd.Timestamp.now("UTC")

    df_funcao = pd.DataFrame(
        {
            "nu_ano_referencia": [year] * len(funcoes),
            "cd_funcao": [r.cd_funcao for r in funcoes],
            "ds_funcao": [r.ds_funcao for r in funcoes],
            "nm_arquivo": nm_arquivo,
            "dt_atualizacao": now,
        }
    )
    df_subfuncao = pd.DataFrame(
        {
            "nu_ano_referencia": [year] * len(subfuncoes),
            "cd_subfuncao": [r.cd_subfuncao for r in subfuncoes],
            "ds_subfuncao": [r.ds_subfuncao for r in subfuncoes],
            "nm_arquivo": nm_arquivo,
            "dt_atualizacao": now,
        }
    )
    return df_funcao, df_subfuncao


@dg.asset(
    partitions_def=year_partition_loa,
    kinds={"zip", "pdf", "pandas", "duckdb"},
    group_name="raw",
)
def pjf_loa_funcao(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    year = int(context.partition_key)
    zip_bytes, zip_path = ensure_loa_zip(year, fs)
    extract_dir = zip_path.parent / str(year)

    df_funcao, _ = read_loa_funcionais(
        zip_bytes=zip_bytes,
        year=year,
        nm_arquivo=zip_path.name,
        extract_dir=extract_dir,
    )

    overwrite_partition_in_duckdb(
        duckdb=duckdb,
        _df=df_funcao,
        schema="stg",
        table="pjf_loa_funcao",
        partition_col="nu_ano_referencia",
        partition_value=year,
    )

    return dg.MaterializeResult()


@dg.asset(
    partitions_def=year_partition_loa,
    deps=[pjf_loa_funcao],
    kinds={"zip", "pdf", "pandas", "duckdb"},
    group_name="raw",
)
def pjf_loa_subfuncao(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    year = int(context.partition_key)
    zip_bytes, zip_path = ensure_loa_zip(year, fs)
    extract_dir = zip_path.parent / str(year)

    _, df_subfuncao = read_loa_funcionais(
        zip_bytes=zip_bytes,
        year=year,
        nm_arquivo=zip_path.name,
        extract_dir=extract_dir,
    )

    overwrite_partition_in_duckdb(
        duckdb=duckdb,
        _df=df_subfuncao,
        schema="stg",
        table="pjf_loa_subfuncao",
        partition_col="nu_ano_referencia",
        partition_value=year,
    )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(assets=[pjf_loa_funcao, pjf_loa_subfuncao])
