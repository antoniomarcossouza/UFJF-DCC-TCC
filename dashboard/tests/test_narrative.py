"""Testes de textos narrativos."""

from __future__ import annotations

from datetime import date

from dashboard.utils.narrative import (
    insight_concentracao_fim_periodo,
    insight_concentracao_pareto,
    insight_eficiencia_execucao,
    insight_estagios_despesa,
    insight_evolucao_anual,
    insight_execucao,
    insight_participacao_transferencias,
    insight_principal_fonte,
    insight_saldo_fiscal,
    insight_saldo_ultimo_mes,
    insight_tendencia_saldo,
    insight_top_fornecedor,
    insight_top_funcao,
    insight_top_unidade,
    insight_yoy,
    pct_concentracao_fim_periodo,
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


def test_insight_saldo_fiscal_positivo():
    s = insight_saldo_fiscal(100.0, 80.0, 20.0)
    assert "cobre" in s.lower() or "saldo" in s.lower()


def test_insight_saldo_fiscal_none():
    s = insight_saldo_fiscal(None, 80.0, None)
    assert "não calculado" in s.lower() or "Saldo fiscal" in s


def test_insight_saldo_ultimo_mes():
    s = insight_saldo_ultimo_mes(100.0, date(2024, 6, 1))
    assert "2024" in s or "06" in s


def test_insight_saldo_ultimo_mes_none():
    s = insight_saldo_ultimo_mes(None, date(2024, 1, 1))
    assert "indisponível" in s.lower()


def test_insight_tendencia_saldo():
    s = insight_tendencia_saldo([1.0, -2.0, 3.0])
    assert "negativo" in s.lower()


def test_insight_tendencia_saldo_vazia():
    s = insight_tendencia_saldo([])
    assert "Sem série" in s or "analisar" in s


def test_insight_estagios_despesa():
    s = insight_estagios_despesa(100.0, 90.0, 80.0)
    assert "empenho" in s.lower() or "Fluxo" in s


def test_insight_estagios_despesa_none():
    s = insight_estagios_despesa(None, 1.0, 1.0)
    assert "incompletos" in s.lower()


def test_insight_eficiencia_execucao():
    s = insight_eficiencia_execucao(90.0, 50.0, 55.0)
    assert "liquidado" in s.lower()


def test_insight_eficiencia_execucao_todos_none():
    s = insight_eficiencia_execucao(None, None, None)
    assert "indisponíveis" in s.lower() or "empenho" in s.lower()


def test_insight_concentracao_fim_periodo():
    rows = [(2024, 1, 10.0), (2024, 2, 10.0), (2024, 3, 80.0)]
    s = insight_concentracao_fim_periodo(rows)
    assert "%" in s or "concentram" in s.lower()


def test_insight_concentracao_fim_periodo_vazio():
    s = insight_concentracao_fim_periodo([])
    assert "Sem série" in s or "suficiente" in s.lower()


def test_pct_concentracao_fim_periodo():
    rows = [(2024, 1, 10.0), (2024, 2, 10.0), (2024, 3, 80.0)]
    p = pct_concentracao_fim_periodo(rows)
    assert p is not None
    assert abs(p - 90.0) < 0.01


def test_pct_concentracao_fim_periodo_curto():
    rows = [(2024, 1, 1.0), (2024, 2, 2.0)]
    assert pct_concentracao_fim_periodo(rows) is None


def test_insight_top_fornecedor():
    s = insight_top_fornecedor("ACME", 1000.0, 40.0)
    assert "ACME" in s


def test_insight_top_fornecedor_vazio():
    s = insight_top_fornecedor("", 1.0, 1.0)
    assert "não identificado" in s.lower()


def test_insight_concentracao_pareto():
    s = insight_concentracao_pareto(70.0, 5)
    assert "5" in s


def test_insight_concentracao_pareto_none():
    s = insight_concentracao_pareto(None, 5)
    assert "não calculada" in s.lower()


def test_insight_top_unidade():
    s = insight_top_unidade("Secretaria X", 500.0, 30.0)
    assert "Secretaria" in s


def test_insight_top_funcao():
    s = insight_top_funcao("Saúde / 10", 200.0, 25.0)
    assert "Saúde" in s or "função" in s.lower()
