"""Testes de execução das queries de receitas contra DuckDB."""

from pathlib import Path

import duckdb
import pytest

from dashboard.queries.filters import FilterState
from dashboard.queries import receitas

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = REPO_ROOT / "data" / "execucao_orcamentaria.duckdb"


@pytest.fixture(scope="module")
def connection():
    if not DB_PATH.exists():
        pytest.skip(f"DuckDB não encontrado: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    yield con
    con.close()


def _assert_executes(con, sql: str, params: list) -> None:
    assert sql.count("?") == len(params), (
        f"placeholders={sql.count('?')} params={len(params)}"
    )
    con.execute(sql, params).fetchdf()


def test_heatmap_executes(connection):
    flt = FilterState(anos=(2026,), meses=(1, 2))
    sql, params = receitas.heatmap_sazonalidade(flt)
    _assert_executes(connection, sql, params)


def test_heatmap_sem_filtro_executes(connection):
    sql, params = receitas.heatmap_sazonalidade(FilterState())
    _assert_executes(connection, sql, params)
