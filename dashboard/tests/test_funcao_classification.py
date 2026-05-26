"""Testes de classificação de função orçamentária."""

from __future__ import annotations

import pytest

from dashboard.queries.funcao_classification import nome_funcao


def test_nome_funcao_saude():
    assert nome_funcao("10") == "Saúde"


def test_nome_funcao_educacao():
    assert nome_funcao("12") == "Educação"


def test_nome_funcao_desconhecido_fallback():
    assert nome_funcao("88") == "Função 88"


def test_nome_funcao_um_digito():
    assert nome_funcao("6") == "Segurança Pública"


def test_nome_funcao_type_error():
    with pytest.raises(TypeError):
        nome_funcao(None)  # type: ignore[arg-type]


def test_nome_funcao_vazio_value_error():
    with pytest.raises(ValueError):
        nome_funcao("   ")


def test_nome_funcao_sem_digitos_value_error():
    with pytest.raises(ValueError):
        nome_funcao("abc")
