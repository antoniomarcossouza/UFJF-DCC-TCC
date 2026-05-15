"""Página: Receitas."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters, kpi, tables
from dashboard.queries import receitas
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_brl, fmt_pct
from dashboard.utils.safe_math import pct

st.set_page_config(page_title="Receitas", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Receitas")
disclaimers.render_data_coverage()

df_kpi = run_query(*receitas.kpis_execucao_receita(flt))
if not df_kpi.empty:
    r = df_kpi.iloc[0]
    exec_pct = pct(r["vl_arrecadada_ano"], r["vl_previsao_atualizada"])
    kpi.kpi_row(
        [
            kpi.kpi_brl("Previsão inicial", float(r["vl_previsao_inicial"])),
            kpi.kpi_brl(
                "Previsão atualizada", float(r["vl_previsao_atualizada"])
            ),
            kpi.kpi_brl("Arrecadado no ano", float(r["vl_arrecadada_ano"])),
            kpi.kpi_pct("% execução da receita", exec_pct),
        ]
    )

df_serie = run_query(*receitas.serie_mensal_arrecadacao(flt))
if not df_serie.empty:
    df_serie["periodo"] = (
        df_serie["sg_mes"] + "/" + df_serie["nu_ano"].astype(str)
    )
    charts.line_series(
        df_serie,
        x="periodo",
        y_cols=["vl_arrecadada", "vl_previsto"],
        labels={
            "vl_arrecadada": "Arrecadado",
            "vl_previsto": "Previsto mensal",
        },
        titulo="Arrecadação mensal vs previsão",
        legenda="Linha azul: arrecadado; laranja: previsto mensal",
        descricao="Identifica sazonalidade e meses de pico ou queda na arrecadação.",
    )

df_heat = run_query(*receitas.heatmap_sazonalidade(flt))
if not df_heat.empty:
    df_heat["natureza"] = (
        df_heat["cd_natureza_receita"] + " - " + df_heat["ds_natureza_receita"]
    )
    charts.heatmap_chart(
        df_heat,
        x="sg_mes",
        y="natureza",
        z="vl_arrecadada",
        titulo="Heatmap: arrecadação por natureza e mês",
        legenda="Eixo X: mês; Y: naturezas top 15; cor: valor arrecadado",
        descricao="Destaca concentração temporal por natureza de receita.",
    )

df_rank = run_query(*receitas.ranking_naturezas(flt))
if not df_rank.empty:
    df_rank["label"] = (
        df_rank["cd_natureza_receita"] + " - " + df_rank["ds_natureza_receita"]
    )
    charts.bar_horizontal(
        df_rank.head(15),
        y="label",
        x="vl_arrecadada",
        titulo="Principais naturezas de receita",
        legenda="Barras: valor arrecadado no período",
        descricao="Ranking das receitas que mais contribuem para o total arrecadado.",
    )

st.subheader("Receitas abaixo do esperado (< 80% da previsão atualizada)")
df_abaixo = run_query(*receitas.receitas_abaixo_previsto(flt))
if not df_abaixo.empty:
    df_show = df_abaixo.copy()
    df_show["pct_realizacao"] = df_show["pct_realizacao"].apply(
        lambda x: fmt_pct(float(x)) if x is not None else "—"
    )
    tables.render_table(
        df_show,
        descricao="Naturezas com baixa realização em relação à previsão atualizada acumulada.",
    )
else:
    st.info("Nenhuma natureza abaixo do limiar de 80% no recorte atual.")

st.subheader("Deduções e impacto na arrecadação líquida")
df_ded = run_query(*receitas.deducoes_receita(flt))
if not df_ded.empty:
    d = df_ded.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total bruto", fmt_brl(float(d["vl_bruto"])))
    c2.metric(
        "Categoria 9 (redutoras)", fmt_brl(float(d["vl_deducoes_categoria9"]))
    )
    c3.metric("Valores negativos", fmt_brl(float(d["vl_valores_negativos"])))
    c4.metric("Somente positivos", fmt_brl(float(d["vl_positivo"])))
    st.caption(
        "Deduções: naturezas cd iniciando em 9 (STN) e/ou valores negativos no fato mensal."
    )
df_det = run_query(*receitas.detalhe_deducoes(flt))
tables.render_table(df_det, titulo="Detalhamento de deduções")
