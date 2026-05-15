"""Página: Execução Orçamentária."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters, tables
from dashboard.queries import unidades
from dashboard.utils.db import run_query

st.set_page_config(page_title="Execução Orçamentária", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Execução Orçamentária")
disclaimers.render_data_coverage()

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
        titulo="Execução por unidade administrativa",
        legenda="Comparativo dos três estágios por unidade",
        descricao="Mostra quais unidades mais executam despesas no período.",
    )
    tables.render_table(df_ua, titulo="Tabela por unidade")

df_func = run_query(*unidades.execucao_por_funcao(flt))
if not df_func.empty:
    df_func["rotulo"] = (
        "Função "
        + df_func["cd_funcao"]
        + " / Sub "
        + df_func["cd_subfuncao"]
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
        titulo="Execução por função e subfunção",
        legenda="Códigos funcionais + estágios da despesa",
        descricao="Áreas como saúde (10) e educação (12) identificadas pelo código de função.",
    )
    tables.render_table(
        df_func,
        titulo="Detalhamento funcional",
        descricao="ds_acao_exemplo: descrição de uma ação representativa do grupo.",
    )
