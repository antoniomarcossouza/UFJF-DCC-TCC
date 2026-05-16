"""Testes de textos narrativos."""

from __future__ import annotations

from dashboard.utils.narrative import (
    insight_evolucao_anual,
    insight_execucao,
    insight_participacao_transferencias,
    insight_principal_fonte,
    insight_yoy,
)


def test_insight_yoy_crescimento():
    s = insight_yoy(100.0, 80.0, ano_atual=2026, ano_anterior=2025)
    assert "cresceu" in s
    assert "25,0%" in s or "25.0%" in s


def test_insight_yoy_queda():
    s = insight_yoy(80.0, 100.0, ano_atual=2026, ano_anterior=2025)
    assert "caiu" in s
    assert "20,0%" in s or "20.0%" in s


def test_insight_yoy_base_zero():
    s = insight_yoy(100.0, 0.0, ano_atual=2026, ano_anterior=2025)
    assert "indisponível" in s.lower()


def test_insight_yoy_none():
    s = insight_yoy(100.0, None, ano_atual=2026, ano_anterior=2025)
    assert "indisponível" in s.lower()


def test_insight_principal_fonte_com_pct():
    s = insight_principal_fonte("Transferências da União", 45.5)
    assert "Transferências da União" in s
    assert "45,5%" in s or "45.5%" in s


def test_insight_execucao_ok():
    s = insight_execucao(78.0, 100.0, 78.0)
    assert "78,0%" in s or "78.0%" in s


def test_insight_execucao_none():
    s = insight_execucao(None, 100.0, None)
    assert "Não foi possível" in s


def test_insight_participacao():
    s = insight_participacao_transferencias(40.0, 10.0)
    assert "federal" in s.lower() or "40" in s


def test_insight_evolucao_anual():
    s = insight_evolucao_anual({2024: 100.0, 2025: 110.0})
    assert "2024" in s
    assert "2025" in s
