"""Página: Visão Geral Fiscal."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters, kpi
from dashboard.queries import fiscal, receitas
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_brl
from dashboard.utils.safe_math import pct

st.set_page_config(page_title="Visão Geral Fiscal", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Visão Geral Fiscal")
disclaimers.render_data_coverage()

df_kpi = run_query(*fiscal.kpis_fiscal(flt))
if not df_kpi.empty:
    row = df_kpi.iloc[0]
    exec_pct = None
    df_rec = run_query(*receitas.kpis_execucao_receita(flt))
    if not df_rec.empty:
        exec_pct = pct(
            df_rec.iloc[0]["vl_arrecadada_ano"],
            df_rec.iloc[0]["vl_previsao_atualizada"],
        )
    kpi.kpi_row(
        [
            kpi.kpi_brl("Arrecadação (filtros)", float(row["vl_arrecadada"])),
            kpi.kpi_brl("Pagamentos (filtros)", float(row["vl_pago"])),
            kpi.kpi_brl("Saldo fiscal", float(row["vl_saldo"])),
            kpi.kpi_pct("% execução receita (ano)", exec_pct),
        ]
    )
    if float(row["vl_arrecadada"]) < float(row["vl_pago"]):
        st.error("Despesas pagas superam arrecadação no recorte filtrado.")
    else:
        st.success("Arrecadação cobre pagamentos no recorte filtrado.")

df_ultimo = run_query(*fiscal.saldo_mensal_ultimo_mes(flt))
if not df_ultimo.empty:
    u = df_ultimo.iloc[0]
    st.subheader("Último mês com referência nos dados")
    c1, c2, c3 = st.columns(3)
    c1.metric("Arrecadação do mês", fmt_brl(float(u["vl_arrecadada_mes"])))
    c2.metric("Pagamentos do mês", fmt_brl(float(u["vl_pago_mes"])))
    saldo = float(u["vl_saldo_mensal"])
    c3.metric("Saldo mensal", fmt_brl(saldo))
    if saldo >= 0:
        st.caption("No último mês: arrecadação ≥ pagamentos.")
    else:
        st.caption("No último mês: pagamentos > arrecadação.")

df_serie = run_query(*fiscal.serie_mensal_fiscal(flt))
if not df_serie.empty:
    df_serie["periodo"] = (
        df_serie["sg_mes"] + "/" + df_serie["nu_ano"].astype(str)
    )
    charts.line_series(
        df_serie,
        x="periodo",
        y_cols=["vl_arrecadada", "vl_pago", "vl_saldo_mensal"],
        labels={
            "vl_arrecadada": "Arrecadação",
            "vl_pago": "Pagamentos",
            "vl_saldo_mensal": "Saldo mensal",
        },
        titulo="Evolução mensal: arrecadação vs pagamentos",
        legenda="Linhas: arrecadação, pagamentos e saldo mensal",
        descricao="Compara entradas e saídas de caixa por mês; saldo negativo indica déficit mensal.",
    )
