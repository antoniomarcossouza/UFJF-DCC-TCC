"""Formatação de valores para exibição (functional core)."""

from __future__ import annotations

from datetime import date


def fmt_brl(value: float | int | None) -> str:
    if value is None:
        return "—"
    return (
        f"R$ {value:,.2f}".replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    if value is None:
        return "—"
    return f"{value:.{decimals}f}%"


def fmt_int(value: int | float | None) -> str:
    if value is None:
        return "—"
    return f"{int(value):,}".replace(",", ".")


def fmt_date(value: date | None) -> str:
    if value is None:
        return "—"
    return value.strftime("%d/%m/%Y")
