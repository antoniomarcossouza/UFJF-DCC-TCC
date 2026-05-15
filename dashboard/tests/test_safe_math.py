"""Testes unitários de safe_math."""

from dashboard.utils.safe_math import divide, pct


def test_divide_normal():
    assert divide(10, 2) == 5.0


def test_divide_zero():
    assert divide(10, 0) is None


def test_divide_none():
    assert divide(None, 5) is None


def test_pct():
    assert pct(25, 100) == 25.0


def test_pct_zero_denominator():
    assert pct(25, 0) is None
