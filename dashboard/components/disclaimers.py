"""Avisos de limitação e cobertura de dados."""

from __future__ import annotations

import streamlit as st

from dashboard.queries import schema
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_date


def render_data_coverage() -> None:
    df = run_query(*schema.data_coverage_sql())
    if df.empty:
        return
    with st.expander("Cobertura e limitações dos dados", expanded=False):
        for _, row in df.iterrows():
            fato = row["fato"]
            dt_min = row["dt_min"]
            dt_max = row["dt_max"]
            qtd = row["qtd_meses"]
            st.markdown(
                f"**{fato}**: {fmt_date(dt_min)} a {fmt_date(dt_max)} "
                f"({qtd} mês(es) distintos)"
            )
