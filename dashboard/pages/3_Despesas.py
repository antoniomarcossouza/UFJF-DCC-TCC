"""Página: Despesas."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters, kpi
from dashboard.queries import despesas
from dashboard.utils.db import run_query

st.set_page_config(page_title="Despesas", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Despesas")
disclaimers.render_data_coverage()
disclaimers.render_orcamento_autorizado_aviso()

df_kpi = run_query(*despesas.kpis_despesa(flt))
df_ef = run_query(*despesas.eficiencia_execucao(flt))
if not df_kpi.empty:
    d = df_kpi.iloc[0]
    kpi.kpi_row(
        [
            kpi.kpi_brl("Empenhado", float(d["vl_empenhado"])),
            kpi.kpi_brl("Liquidado", float(d["vl_liquidado"])),
            kpi.kpi_brl("Pago", float(d["vl_pago"])),
        ]
    )
if not df_ef.empty:
    e = df_ef.iloc[0]
    kpi.kpi_row(
        [
            kpi.kpi_pct("% liquidado / empenhado", float(e["pct_liquidado_sobre_empenhado"])
            if e["pct_liquidado_sobre_empenhado"] is not None
            else None),
            kpi.kpi_pct("% pago / empenhado", float(e["pct_pago_sobre_empenhado"])
            if e["pct_pago_sobre_empenhado"] is not None
            else None),
            kpi.kpi_pct("% pago / liquidado", float(e["pct_pago_sobre_liquidado"])
            if e["pct_pago_sobre_liquidado"] is not None
            else None),
        ]
    )

df_serie = run_query(*despesas.serie_mensal_despesa(flt))
if not df_serie.empty:
    df_serie["periodo"] = df_serie["sg_mes"] + "/" + df_serie["nu_ano"].astype(str)
    charts.line_series(
        df_serie,
        x="periodo",
        y_cols=["vl_empenhado", "vl_liquidado", "vl_pago"],
        labels={
            "vl_empenhado": "Empenhado",
            "vl_liquidado": "Liquidado",
            "vl_pago": "Pago",
        },
        titulo="Evolução da execução da despesa",
        legenda="Três estágios da despesa pública municipal",
        descricao="Acompanha empenho, liquidação e pagamento ao longo do tempo.",
    )

df_func = run_query(*despesas.distribuicao_funcional(flt))
if not df_func.empty:
    df_func["rotulo"] = (
        "Função "
        + df_func["cd_funcao"]
        + " / Sub "
        + df_func["cd_subfuncao"]
        + " — "
        + df_func["ds_funcional_pragmatica"].str.slice(0, 40)
    )
    charts.treemap_chart(
        df_func,
        path=["cd_funcao", "cd_subfuncao"],
        values="vl_pago",
        titulo="Distribuição do gasto por função e subfunção",
        legenda="Área proporcional ao valor pago",
        descricao="Mostra onde os recursos foram aplicados (códigos + descrição da ação).",
    )

df_nat = run_query(*despesas.ranking_naturezas_despesa(flt))
if not df_nat.empty:
    df_nat["label"] = df_nat["cd_natureza_despesa"] + " - " + df_nat["ds_natureza_despesa"]
    charts.bar_horizontal(
        df_nat.head(15),
        y="label",
        x="vl_pago",
        titulo="Naturezas de despesa que mais consomem recursos",
        legenda="Valor pago por natureza",
        descricao="Ranking de categorias econômicas de gasto (pessoal, material, etc.).",
    )

df_acc = run_query(*despesas.aceleracao_gastos(flt))
if not df_acc.empty and len(df_acc) > 1:
    df_acc["periodo"] = df_acc["sg_mes"] + "/" + df_acc["nu_ano"].astype(str)
    charts.line_series(
        df_acc,
        x="periodo",
        y_cols=["vl_pago"],
        labels={"vl_pago": "Pagamentos"},
        titulo="Tendência de pagamentos (aceleração)",
        legenda="Série de pagamentos mensais",
        descricao="Picos no fim do período podem indicar concentração de gastos.",
    )
