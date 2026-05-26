"""Tabelas analíticas."""

from __future__ import annotations

import io

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


def download_csv(
    df: pd.DataFrame,
    filename: str = "dados.csv",
    *,
    key: str | None = None,
) -> None:
    if df.empty:
        return
    st.download_button(
        "Baixar CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        key=key,
    )


def download_xlsx(
    df: pd.DataFrame,
    filename: str = "dados.xlsx",
    *,
    key: str | None = None,
) -> None:
    if df.empty:
        return
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    st.download_button(
        "Baixar XLSX",
        data=buf.getvalue(),
        file_name=filename,
        mime=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        key=key,
    )
