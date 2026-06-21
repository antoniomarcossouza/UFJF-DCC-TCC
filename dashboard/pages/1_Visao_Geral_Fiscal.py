"""Página: Visão Geral Fiscal (visão cidadão)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, filters
from dashboard.queries import fiscal, receitas
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_brl, fmt_brl_compact, fmt_pct
from dashboard.utils.glossary import termo
from dashboard.utils.safe_math import pct

st.set_page_config(page_title="Visão Geral Fiscal", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Visão Geral Fiscal")
st.caption(
    "Veja se a prefeitura tem fechado o caixa no azul ou no vermelho"
)

df_kpi = run_query(*fiscal.kpis_fiscal(flt))
exec_pct = None
vl_arrec = None
vl_pago = None
vl_saldo = None
if not df_kpi.empty:
    row = df_kpi.iloc[0]
    vl_arrec = float(row["vl_arrecadada"])
    vl_pago = float(row["vl_pago"])
    vl_saldo = float(row["vl_saldo"])
    df_rec = run_query(*receitas.kpis_execucao_receita(flt))
    if not df_rec.empty:
        exec_pct = pct(
            float(df_rec.iloc[0]["vl_arrecadada_ano"]),
            float(df_rec.iloc[0]["vl_previsao_atualizada"]),
        )

with st.expander("O que você quer descobrir?", expanded=False):
    st.markdown(
        """
- **Está sobrando ou faltando dinheiro?** Seção 1.
- **Como foi o último mês?** Seção 2.
- **Como variou ao longo do tempo?** Seção 3.
        """
    )

st.markdown('<a id="fiscal-s1"></a>', unsafe_allow_html=True)
st.subheader("1. Resumo")
_, r_rec = termo("receita_realizada")
_, r_pag = termo("pagamento")
_, r_saldo = termo("saldo_fiscal")
_, r_prev = termo("previsao_atualizada")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric(
        "Arrecadação (recorte)",
        fmt_brl_compact(vl_arrec),
        help=r_rec,
    )
with c2:
    st.metric(
        "Pagamentos (recorte)",
        fmt_brl_compact(vl_pago),
        help=r_pag,
    )
with c3:
    st.metric(
        "Saldo fiscal (recorte)",
        fmt_brl_compact(vl_saldo),
        help=r_saldo,
    )
with c4:
    st.metric(
        "% execução da receita (meta)",
        fmt_pct(exec_pct),
        help=r_prev,
    )

st.divider()
st.markdown('<a id="fiscal-s2"></a>', unsafe_allow_html=True)
st.subheader("2. Último mês com referência nos dados")
st.caption(
    "Compara arrecadação e pagamentos no mês mais recente com dado dentro "
    "do recorte (ano e demais filtros da barra lateral; o filtro de mês "
    "não se aplica aqui)."
)
df_ultimo = run_query(*fiscal.saldo_mensal_ultimo_mes(flt))
if not df_ultimo.empty:
    u = df_ultimo.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Arrecadação do mês", fmt_brl(float(u["vl_arrecadada_mes"])))
    c2.metric("Pagamentos do mês", fmt_brl(float(u["vl_pago_mes"])))
    saldo_m = float(u["vl_saldo_mensal"])
    c3.metric("Saldo mensal", fmt_brl(saldo_m))
else:
    st.info("Sem dados de último mês para o recorte atual.")

st.divider()
st.markdown('<a id="fiscal-s3"></a>', unsafe_allow_html=True)
st.subheader("3. Evolução mensal")
df_serie = run_query(*fiscal.serie_mensal_fiscal(flt))
if not df_serie.empty:
    df_serie["periodo"] = (
        df_serie["sg_mes"] + "/" + df_serie["nu_ano"].astype(str)
    )
    charts.line_series(
        df_serie,
        x="periodo",
        y_cols=["vl_arrecadada", "vl_pago"],
        labels={
            "vl_arrecadada": "Arrecadação",
            "vl_pago": "Pagamentos",
        },
        titulo="Evolução mensal: arrecadação vs pagamentos",
        legenda="Linhas: arrecadação, pagamentos e saldo mensal",
        descricao="Compara entradas e saídas por mês",
        x_label="Período",
        y_log_scale=True,
    )
else:
    st.info("Sem série mensal para o filtro atual.")
