"""Página: Fornecedores."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters, tables
from dashboard.queries import fornecedores
from dashboard.utils.db import run_query

st.set_page_config(page_title="Fornecedores", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Fornecedores")
disclaimers.render_data_coverage()

df_rank = run_query(*fornecedores.ranking_fornecedores(flt))
if not df_rank.empty:
    df_rank["label"] = df_rank["nm_fornecedor"].fillna(df_rank["cd_cpf_cnpj"])
    charts.bar_horizontal(
        df_rank.head(15),
        y="label",
        x="vl_pago",
        titulo="Fornecedores que mais receberam pagamentos",
        legenda="Valor total pago no período",
        descricao="Identifica concentração de recursos em poucos fornecedores.",
    )
    charts.pareto_chart(
        df_rank.head(20),
        x="label",
        y="vl_pago",
        y2="pct_acumulado",
        titulo="Curva de concentração (Pareto)",
        legenda="Barras: valor; linha: % acumulado do total pago",
        descricao="Se poucos fornecedores concentram grande parte dos pagamentos, há risco de dependência.",
    )
    tables.render_table(
        df_rank,
        titulo="Ranking completo",
        descricao="Inclui quantidade de empenhos e participação percentual.",
    )

st.subheader("Evolução por fornecedor")
if not df_rank.empty:
    labels = {
        r["sk_fornecedor"]: f"{r['nm_fornecedor']} ({r['cd_cpf_cnpj']})"
        for _, r in df_rank.head(50).iterrows()
    }
    sk_sel = st.selectbox(
        "Selecione o fornecedor",
        options=list(labels.keys()),
        format_func=lambda k: labels[k],
    )
    if sk_sel:
        df_evo = run_query(*fornecedores.evolucao_fornecedor(flt, sk_sel))
        if not df_evo.empty:
            df_evo["periodo"] = (
                df_evo["sg_mes"] + "/" + df_evo["nu_ano"].astype(str)
            )
            charts.line_series(
                df_evo,
                x="periodo",
                y_cols=["vl_pago"],
                labels={"vl_pago": "Pago"},
                titulo="Evolução temporal de pagamentos",
                legenda="Pagamentos mensais ao fornecedor selecionado",
                descricao="Acompanha tendência de pagamentos e volume de empenhos distintos.",
            )
            st.metric(
                "Empenhos distintos (período)",
                int(df_evo["qtd_empenhos"].sum()),
            )
