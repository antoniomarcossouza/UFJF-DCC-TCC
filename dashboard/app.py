"""Dashboard de execução orçamentária municipal — entrada Streamlit."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

from dashboard.components.disclaimers import render_data_coverage
from dashboard.components.filters import render_sidebar_filters
from dashboard.queries import schema
from dashboard.utils.db import run_query

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


st.set_page_config(
    page_title="Execução Orçamentária Municipal",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

filters = render_sidebar_filters()

st.title("Execução Orçamentária Municipal")
st.markdown(
    "Dashboard analítico de **receitas** e **despesas** "
)

cols_df = run_query(*schema.introspect_tables_sql())
errors = schema.validate_contract(cols_df)
if errors:
    for err in errors:
        st.error(err)
else:
    st.success("Contrato de dados validado.")

render_data_coverage()

st.markdown("---")
st.markdown(
    """
    Use o menu lateral para navegar:

    - **Visão Geral Fiscal** — equilíbrio receita x despesa
    - **Receitas** — arrecadação, sazonalidade, deduções
    - **Despesas** — empenho, liquidação, pagamento
    - **Fornecedores** — concentração e ranking
    - **Execução Orçamentária** — unidades e funções
    - **Indicadores Per Capita** — métricas por habitante
    - **Exploração Detalhada** — tabelas exportáveis
    """
)
