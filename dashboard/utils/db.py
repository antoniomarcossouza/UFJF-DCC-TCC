"""Shell imperativo: única camada de IO com DuckDB."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "execucao_orcamentaria.duckdb"
SCHEMA = "dwh"


def get_db_path() -> Path:
    env = os.environ.get("DUCKDB_PATH")
    if env:
        return Path(env)
    return DEFAULT_DB_PATH


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    path = get_db_path()
    if not path.exists():
        msg = f"Banco DuckDB não encontrado: {path}"
        raise FileNotFoundError(msg)
    return duckdb.connect(str(path), read_only=True)


@st.cache_data(show_spinner=False)
def run_query(sql: str, params: tuple | list | None = None) -> pd.DataFrame:
    con = get_connection()
    if params:
        return con.execute(sql, params).fetchdf()
    return con.execute(sql).fetchdf()


def fq(table: str) -> str:
    return f"{SCHEMA}.{table}"
