"""Página: Execução Orçamentária (visão cidadão)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters, tables
from dashboard.components import glossary as glossary_ui
from dashboard.components import narrative as narrative_ui
from dashboard.queries import unidades
from dashboard.queries.funcao_classification import nome_funcao
from dashboard.utils.db import run_query
from dashboard.utils.glossary import termo
from dashboard.utils.narrative import insight_top_funcao, insight_top_unidade
from dashboard.utils.safe_math import pct as pct_safe

st.set_page_config(page_title="Execução Orçamentária", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Execução Orçamentária")
st.caption(
    "Compare quais secretarias e áreas de governo mais executam o "
    "orçamento (empenho, liquidação e pagamento)."
)
disclaimers.render_data_coverage()
disclaimers.render_orcamento_autorizado_aviso()

with st.expander("O que você quer descobrir?", expanded=False):
    st.markdown(
        """
- **Quais secretarias mais gastam?** — seção 1 (unidades administrativas).
- **Quais áreas (saúde, educação…) mais consomem?** — seção 2 (função).
        """
    )
st.caption(
    "Os números seguem o recorte escolhido nos filtros da barra lateral."
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
    row_u = df_ua.sort_values("vl_pago", ascending=False).iloc[0]
    total_u = float(df_ua["vl_pago"].sum())
    pct_u = pct_safe(float(row_u["vl_pago"]), total_u)
    _, h_ua = termo("unidade_administrativa")
    narrative_ui.insight_box(
        insight_top_unidade(
            str(row_u["nm_unidade_administrativa"]),
            float(row_u["vl_pago"]),
            pct_u,
        ),
        tone="info",
    )
    tables.render_table(
        df_ua,
        titulo="Tabela por unidade",
        descricao=h_ua,
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
    row_f = df_func.sort_values("vl_pago", ascending=False).iloc[0]
    total_f = float(df_func["vl_pago"].sum())
    pct_f = pct_safe(float(row_f["vl_pago"]), total_f)
    label_f = (
        f"{nome_funcao(str(row_f['cd_funcao']))} / sub {row_f['cd_subfuncao']}"
    )
    narrative_ui.insight_box(
        insight_top_funcao(label_f, float(row_f["vl_pago"]), pct_f),
        tone="info",
    )
    tables.render_table(
        df_func,
        titulo="Detalhamento funcional",
        descricao="ds_acao_exemplo: descrição de uma ação representativa do "
        "grupo.",
    )
    glossary_ui.glossary_expander()
else:
    st.info("Sem execução funcional no recorte.")
