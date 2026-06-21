from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

DASHBOARD = Path(__file__).resolve().parent

st.set_page_config(
    page_title="Execução Orçamentária Municipal",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = [
    st.Page(
        DASHBOARD / "home.py",
        title="Início",
        default=True,
    ),
    st.Page(
        DASHBOARD / "pages/1_Visao_Geral_Fiscal.py",
        title="Visão Geral Fiscal",
    ),
    st.Page(
        DASHBOARD / "pages/2_Receitas.py",
        title="Receitas",
    ),
    st.Page(
        DASHBOARD / "pages/3_Despesas.py",
        title="Despesas",
    ),
    st.Page(
        DASHBOARD / "pages/4_Fornecedores.py",
        title="Fornecedores",
    ),
    st.Page(
        DASHBOARD / "pages/5_Execucao_Orcamentaria.py",
        title="Execução Orçamentária",
    ),
]

pg = st.navigation(pages)
pg.run()
