"""Página: Visão Geral Fiscal (visão cidadão)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, disclaimers, filters
from dashboard.components import glossary as glossary_ui
from dashboard.components import narrative as narrative_ui
from dashboard.queries import fiscal, receitas
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_brl, fmt_brl_compact, fmt_pct
from dashboard.utils.glossary import termo
from dashboard.utils.narrative import (
    insight_saldo_fiscal,
    insight_saldo_ultimo_mes,
    insight_tendencia_saldo,
)
from dashboard.utils.safe_math import pct

st.set_page_config(page_title="Visão Geral Fiscal", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Visão Geral Fiscal")
st.caption(
    "Veja se a prefeitura tem fechado o caixa no azul ou no vermelho — "
    "quanto entrou, quanto saiu e o saldo."
)
disclaimers.render_data_coverage()

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
- **Está sobrando ou faltando dinheiro?** — seção 1 (resumo).
- **Como foi o último mês?** — seção 2 (último mês com dado).
- **Como variou ao longo do tempo?** — seção 3 (evolução mensal).
        """
    )
st.caption(
    "Os números seguem o recorte escolhido nos filtros da barra lateral "
    "(ano, mês, unidade, função, naturezas, fontes, fornecedores)."
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

if vl_arrec is not None and vl_pago is not None and vl_saldo is not None:
    tone = "success" if vl_saldo >= 0 else "warning"
    narrative_ui.insight_box(
        insight_saldo_fiscal(vl_arrec, vl_pago, vl_saldo),
        tone=tone,
    )
    if vl_arrec < vl_pago:
        st.error("Despesas pagas superam arrecadação no recorte filtrado.")
    else:
        st.success("Arrecadação cobre pagamentos no recorte filtrado.")

st.divider()
st.markdown('<a id="fiscal-s2"></a>', unsafe_allow_html=True)
st.subheader("2. Último mês com referência nos dados")
st.caption(
    "Compara arrecadação e pagamentos apenas no mês mais recente presente "
    "na base (independente do filtro de ano, se houver dados)."
)
df_ultimo = run_query(*fiscal.saldo_mensal_ultimo_mes(flt))
if not df_ultimo.empty:
    u = df_ultimo.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Arrecadação do mês", fmt_brl(float(u["vl_arrecadada_mes"])))
    c2.metric("Pagamentos do mês", fmt_brl(float(u["vl_pago_mes"])))
    saldo_m = float(u["vl_saldo_mensal"])
    c3.metric("Saldo mensal", fmt_brl(saldo_m))
    dt_ref = u["dt_ref"]
    narrative_ui.insight_box(
        insight_saldo_ultimo_mes(saldo_m, dt_ref),
        tone="success" if saldo_m >= 0 else "warning",
    )
else:
    st.info("Sem dados de último mês para o recorte atual.")

st.divider()
st.markdown('<a id="fiscal-s3"></a>', unsafe_allow_html=True)
st.subheader("3. Evolução mensal")
st.caption(
    "Linhas: quanto entrou (arrecadação), quanto saiu em pagamentos e o "
    "saldo mês a mês. Saldo negativo = naquele mês saiu mais do que entrou."
)
df_serie = run_query(*fiscal.serie_mensal_fiscal(flt))
if not df_serie.empty:
    df_serie["periodo"] = (
        df_serie["sg_mes"] + "/" + df_serie["nu_ano"].astype(str)
    )
    saldos = [float(x) for x in df_serie["vl_saldo_mensal"].tolist()]
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
        descricao="Compara entradas e saídas por mês; saldo negativo indica "
        "déficit mensal.",
        x_label="Período",
    )
    narrative_ui.insight_box(
        insight_tendencia_saldo(saldos),
        tone="info",
    )
    glossary_ui.glossary_expander()
else:
    st.info("Sem série mensal para o filtro atual.")
