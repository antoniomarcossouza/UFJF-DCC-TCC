from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

from dashboard.components.filters import render_sidebar_filters  # noqa: E402

st.set_page_config(
    page_title="Execução Orçamentária Municipal",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

filters = render_sidebar_filters()

st.title("Execução Orçamentária Municipal")
st.markdown(
    "Dashboard analítico de receitas e despesas da Prefeitura de Juiz de Fora"
)

st.divider()
st.markdown(
    """
    Use o menu lateral para navegar:

    - Visão Geral Fiscal: Equilíbrio receitas x despesas
    - Receitas: Arrecadação, sazonalidade, deduções
    - Despesas: Empenho, liquidação, pagamento
    - Fornecedores: Concentração e ranking
    - Execução Orçamentária: Unidades e funções
    """
)
