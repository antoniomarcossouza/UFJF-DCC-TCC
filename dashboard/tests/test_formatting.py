"""Testes de formatação."""

from __future__ import annotations

from dashboard.utils.formatting import (
    fmt_brl_compact,
    fmt_delta_pct,
    limpar_rotulo_natureza_despesa,
    limpar_rotulo_natureza_receita,
)


def test_fmt_brl_compact_milhoes():
    assert fmt_brl_compact(125_400_000) == "R$ 125,4 mi"


def test_fmt_brl_compact_bilhoes():
    assert fmt_brl_compact(1_200_000_000) == "R$ 1,2 bi"


def test_fmt_brl_compact_mil():
    assert fmt_brl_compact(850_000) == "R$ 850,0 mil"


def test_fmt_brl_compact_pequeno():
    assert fmt_brl_compact(423.12) == "R$ 423,12"


def test_fmt_brl_compact_none():
    assert fmt_brl_compact(None) == "—"


def test_fmt_brl_compact_negativo():
    s = fmt_brl_compact(-1_500_000)
    assert s.startswith("R$ -")
    assert "mi" in s


def test_fmt_delta_pct_positivo():
    assert fmt_delta_pct(12.4) == "+12,4%"


def test_fmt_delta_pct_negativo():
    assert fmt_delta_pct(-3.1) == "-3,1%"


def test_fmt_delta_pct_none():
    assert fmt_delta_pct(None) == "—"


def test_limpar_rotulo_natureza_com_prefixo():
    raw = "16110101.0 - SERVICOS ADMINISTRATIVOS E COMERCIAIS GERAIS"
    assert limpar_rotulo_natureza_receita(raw) == (
        "SERVICOS ADMINISTRATIVOS E COMERCIAIS GERAIS"
    )


def test_limpar_rotulo_natureza_sem_prefixo():
    assert limpar_rotulo_natureza_receita("Só descrição") == "Só descrição"


def test_limpar_rotulo_natureza_none():
    assert limpar_rotulo_natureza_receita(None) == "—"


def test_limpar_rotulo_natureza_despesa_prefixo():
    raw = "3390390000000000 - MATERIAL DE CONSUMO"
    assert limpar_rotulo_natureza_despesa(raw) == "MATERIAL DE CONSUMO"


def test_limpar_rotulo_natureza_despesa_sem_prefixo():
    assert limpar_rotulo_natureza_despesa("Só texto") == "Só texto"
