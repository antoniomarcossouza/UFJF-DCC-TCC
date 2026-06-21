"""Página: Execução Orçamentária (visão cidadão)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, filters, tables
from dashboard.queries import unidades
from dashboard.queries.funcao_classification import nome_funcao
from dashboard.utils.db import run_query
from dashboard.utils.glossary import termo

st.set_page_config(page_title="Execução Orçamentária", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Execução Orçamentária")
st.caption(
    "Compare quais secretarias e áreas de governo mais executam o "
    "orçamento (empenho, liquidação e pagamento)."
)


with st.expander("O que você quer descobrir?", expanded=False):
    st.markdown(
        """
- **Quais secretarias mais gastam?** Seção 1.
- **Quais áreas (saúde, educação…) mais consomem?** Seção 2.
        """
    )

st.markdown('<a id="exec-s1"></a>', unsafe_allow_html=True)
st.subheader("1. Por unidade administrativa")
st.caption(
    "Cada barra é uma secretaria ou órgão; compara empenhado, liquidado e "
    "pago no período."
)
df_ua = run_query(*unidades.execucao_por_unidade(flt))
if not df_ua.empty:
    charts.grouped_bar(
        df_ua.head(20),
        x="nm_unidade_administrativa",
        y_cols=["vl_empenhado", "vl_liquidado", "vl_pago"],
        labels={
            "vl_empenhado": "Empenhado",
            "vl_liquidado": "Liquidado",
            "vl_pago": "Pago",
        },
        titulo="Execução por unidade administrativa (top 20)",
        legenda="Comparativo dos três estágios por unidade",
        descricao="Mostra quais unidades mais executam despesas no período.",
    )
    _, h_ua = termo("unidade_administrativa")
    tables.render_table(
        df_ua,
        titulo="Tabela por unidade",
        descricao=h_ua,
        column_config={
            "nm_unidade_administrativa": st.column_config.TextColumn(
                label="Unidade Administrativa"
            ),
            "vl_empenhado": st.column_config.NumberColumn(
                "Empenhado",
                format="R$ %.2f",
            ),
            "vl_liquidado": st.column_config.NumberColumn(
                "Liquidado",
                format="R$ %.2f",
            ),
            "vl_pago": st.column_config.NumberColumn(
                "Pago",
                format="R$ %.2f",
            ),
        },
    )
else:
    st.info("Sem execução por unidade no recorte.")

st.divider()
st.markdown('<a id="exec-s2"></a>', unsafe_allow_html=True)
st.subheader("2. Por função e subfunção")
st.caption(
    "Função = grande área (código de duas posições, ex.: 10 saúde). "
    "Subfunção detalha dentro da função."
)
df_func = run_query(*unidades.execucao_por_funcao(flt))
if not df_func.empty:
    df_func["rotulo"] = (
        df_func["cd_funcao"].astype(str).map(nome_funcao)
        + " — sub "
        + df_func["cd_subfuncao"].astype(str)
    )
    charts.grouped_bar(
        df_func.head(20),
        x="rotulo",
        y_cols=["vl_empenhado", "vl_liquidado", "vl_pago"],
        labels={
            "vl_empenhado": "Empenhado",
            "vl_liquidado": "Liquidado",
            "vl_pago": "Pago",
        },
        titulo="Execução por função e subfunção (top 20)",
        legenda="Áreas de governo + estágios da despesa",
        descricao="Ex.: saúde (10) e educação (12) aparecem pelo código de "
        "função.",
    )
    tables.render_table(
        df_func,
        titulo="Detalhamento funcional",
        descricao="ds_acao_exemplo: descrição de uma ação representativa do "
        "grupo.",
    )
else:
    st.info("Sem execução funcional no recorte.")
