"""Página inicial do dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

from dashboard.components.filters import render_sidebar_filters  # noqa: E402

st.set_page_config(page_title="Início", layout="wide")

render_sidebar_filters()

st.title("Execução Orçamentária Municipal")
st.markdown(
    "Dashboard analítico de receitas e despesas da Prefeitura de Juiz de Fora"
)

st.divider()
st.markdown(
    """
    Use o menu lateral para navegar:

    - **Visão Geral Fiscal:** equilíbrio receitas x despesas
    - **Receitas:** arrecadação, sazonalidade, deduções
    - **Despesas:** empenho, liquidação, pagamento
    - **Fornecedores:** concentração e ranking
    - **Execução Orçamentária:** unidades e funções
    - **Glossário:** termos orçamentários em linguagem simples
    """
)
