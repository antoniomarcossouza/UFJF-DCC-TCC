"""Funções puras de cálculo seguro (functional core)."""

from __future__ import annotations

from decimal import Decimal
from typing import TypeVar

Numeric = TypeVar("Numeric", int, float, Decimal)


def divide(
    numerator: Numeric | None, denominator: Numeric | None
) -> float | None:
    """Divisão segura; retorna None se denominador nulo ou zero."""
    if numerator is None or denominator is None:
        return None
    denom = float(denominator)
    if denom == 0.0:
        return None
    return float(numerator) / denom


def pct(
    numerator: Numeric | None, denominator: Numeric | None
) -> float | None:
    """Percentual (0-100); None se divisão inválida."""
    ratio = divide(numerator, denominator)
    if ratio is None:
        return None
    return ratio * 100.0
