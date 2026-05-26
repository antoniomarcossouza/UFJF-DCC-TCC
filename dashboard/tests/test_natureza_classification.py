"""Testes da classificação de natureza de receita."""

from __future__ import annotations

import pytest

from dashboard.queries.natureza_classification import (
    ORIGEM_DESCRICAO,
    ORIGEM_LABEL,
    aggregate_vl_por_origem,
    classify_origem,
)


def test_impostos_proprios():
    assert classify_origem("11125001") == "impostos_proprios"


def test_transf_federais_fpm():
    assert classify_origem("17115111") == "transf_federais"


def test_transf_estaduais_icms():
    assert classify_origem("17215001") == "transf_estaduais"


def test_deducoes():
    assert classify_origem("97215001") == "deducoes"


def test_capital():
    assert classify_origem("23000000") == "capital"


def test_intra():
    assert classify_origem("71100000") == "intra"


def test_outras_transf_170():
    assert classify_origem("17000000") == "outras_transf"


def test_outras_correntes_fundeb_codigo_13():
    assert classify_origem("13210103") == "outras_correntes"


def test_fallback_codigo_desconhecido():
    assert classify_origem("00000000") == "outras_correntes"


def test_cd_vazio_erro():
    with pytest.raises(ValueError, match="vazio"):
        classify_origem("")


def test_aggregate_vl_por_origem():
    rows = [("171", 100.0), ("171151", 50.0), ("111", 30.0)]
    d = aggregate_vl_por_origem(rows)
    assert d["transf_federais"] == 150.0
    assert d["impostos_proprios"] == 30.0


def test_labels_completos():
    for key in ORIGEM_LABEL:
        assert key in ORIGEM_DESCRICAO
        assert len(ORIGEM_LABEL[key]) > 0
        assert len(ORIGEM_DESCRICAO[key]) > 0
