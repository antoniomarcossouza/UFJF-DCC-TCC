"""Página: Exploração Detalhada."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import disclaimers, filters, tables
from dashboard.queries import exploracao
from dashboard.utils.db import run_query

st.set_page_config(page_title="Exploração Detalhada", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Exploração Detalhada")
disclaimers.render_data_coverage()

limite = st.slider(
    "Limite de linhas", min_value=100, max_value=2000, value=500, step=100
)
aba = st.radio("Fato", ["Receitas", "Despesas"], horizontal=True)

if aba == "Receitas":
    df = run_query(*exploracao.detalhe_receita(flt, limite))
    tables.render_table(df, titulo="Detalhe de receitas")
    tables.download_csv(df, "receitas_detalhe.csv")
else:
    df = run_query(*exploracao.detalhe_despesa(flt, limite))
    tables.render_table(df, titulo="Detalhe de despesas")
    tables.download_csv(df, "despesas_detalhe.csv")
