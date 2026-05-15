"""Testes do builder de filtros (functional core)."""

import numpy as np

from dashboard.components.filters import _normalize_key, _normalize_label
from dashboard.queries import receitas
from dashboard.queries.filters import (
    FilterState,
    build_despesa_where,
    build_receita_where,
)


def test_normalize_label_null_rotulo():
    assert _normalize_label(None, "abc123") == "abc123"


def test_normalize_key_numpy_int():
    assert _normalize_key(np.int32(2026)) == 2026


def test_receita_where_sem_filtro():
    params: list = []
    where = build_receita_where(FilterState(), params)
    assert where == "1=1"
    assert params == []


def test_receita_where_com_ano():
    params: list = []
    where = build_receita_where(FilterState(anos=(2026,)), params)
    assert "nu_ano" in where
    assert params == [2026]


def test_despesa_where_com_funcao():
    params: list = []
    where = build_despesa_where(FilterState(cd_funcoes=("10", "12")), params)
    assert "substr" in where
    assert params == ["10", "12"]


def test_heatmap_params_match_placeholders():
    flt = FilterState(anos=(2026,), meses=(1, 2))
    sql, params = receitas.heatmap_sazonalidade(flt)
    assert sql.count("?") == len(params)


def test_despesa_multiplos_filtros():
    params: list = []
    flt = FilterState(anos=(2026,), meses=(1, 2), sk_fornecedores=("abc",))
    where = build_despesa_where(flt, params)
    assert "nu_ano" in where
    assert "nu_mes" in where
    assert "sk_fornecedor" in where
    assert params == [2026, 1, 2, "abc"]
