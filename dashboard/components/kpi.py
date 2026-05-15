"""Componentes de KPI cards."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.formatting import fmt_brl, fmt_pct


def kpi_row(items: list[tuple[str, str, str | None]]) -> None:
    """Exibe linha de KPIs: (label, valor formatado, delta opcional)."""
    cols = st.columns(len(items))
    for col, (label, value, help_text) in zip(cols, items, strict=True):
        col.metric(label, value, help=help_text)


def kpi_brl(
    label: str, value: float | None, help_text: str | None = None
) -> tuple[str, str, str | None]:
    return (label, fmt_brl(value), help_text)


def kpi_pct(
    label: str, value: float | None, help_text: str | None = None
) -> tuple[str, str, str | None]:
    return (label, fmt_pct(value), help_text)
