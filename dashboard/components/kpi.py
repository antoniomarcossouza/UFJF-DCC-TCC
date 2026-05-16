"""Componentes de KPI cards."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.formatting import fmt_brl, fmt_brl_compact, fmt_pct


def kpi_row(
    items: list[
        tuple[str, str, str | None]
        | tuple[str, str, str | None, str | None]
    ],
) -> None:
    """KPIs: (label, value, help) ou (label, value, help, delta)."""
    cols = st.columns(len(items))
    for col, item in zip(cols, items, strict=True):
        if len(item) == 4:
            label, value, help_text, delta = item
        else:
            label, value, help_text = item
            delta = None
        col.metric(label, value, delta=delta, help=help_text)


def kpi_brl(
    label: str, value: float | None, help_text: str | None = None
) -> tuple[str, str, str | None]:
    return (label, fmt_brl(value), help_text)


def kpi_pct(
    label: str, value: float | None, help_text: str | None = None
) -> tuple[str, str, str | None]:
    return (label, fmt_pct(value), help_text)


def kpi_brl_compact(
    label: str,
    value: float | None,
    help_text: str | None = None,
    delta: str | None = None,
) -> tuple[str, str, str | None, str | None]:
    return (label, fmt_brl_compact(value), help_text, delta)


def kpi_text(
    label: str,
    value: str,
    help_text: str | None = None,
    delta: str | None = None,
) -> tuple[str, str, str | None, str | None]:
    return (label, value, help_text, delta)
