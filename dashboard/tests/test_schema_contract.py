"""Teste de contrato de schema contra DuckDB."""

from pathlib import Path

import duckdb
import pytest

from dashboard.queries.schema import EXPECTED_COLUMNS, validate_contract

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = REPO_ROOT / "data" / "execucao_orcamentaria.duckdb"


@pytest.fixture(scope="module")
def columns_df():
    if not DB_PATH.exists():
        pytest.skip(f"DuckDB não encontrado: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(
        """
        select table_name, column_name, data_type
        from information_schema.columns
        where table_schema = 'dwh'
        order by table_name, ordinal_position
        """
    ).fetchdf()
    con.close()
    return df


def test_expected_tables_present(columns_df):
    tables = set(columns_df["table_name"])
    for table in EXPECTED_COLUMNS:
        assert table in tables, f"Tabela ausente: {table}"


def test_contract_no_errors(columns_df):
    errors = validate_contract(columns_df)
    assert errors == [], f"Erros de contrato: {errors}"
