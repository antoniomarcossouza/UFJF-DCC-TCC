"""Tabelas analíticas."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def render_table(
    df: pd.DataFrame,
    *,
    titulo: str | None = None,
    descricao: str | None = None,
    column_config: dict | None = None,
) -> None:
    if titulo:
        st.subheader(titulo)
    if descricao:
        st.caption(descricao)
    if df.empty:
        st.info("Nenhum registro encontrado.")
        return
    st.dataframe(df, width="stretch", column_config=column_config or {})
