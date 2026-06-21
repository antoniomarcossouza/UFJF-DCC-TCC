"""Página: Despesas (visão cidadão)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, filters, kpi
from dashboard.queries import despesas
from dashboard.utils.db import run_query
from dashboard.utils.formatting import limpar_rotulo_natureza_despesa
from dashboard.utils.glossary import termo

st.set_page_config(page_title="Despesas", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Despesas")
st.caption(
    "Acompanhe o caminho do dinheiro público: do empenho até o pagamento."
)

with st.expander("O que você quer descobrir?", expanded=False):
    st.markdown(
        """
- Quanto já saiu efetivamente do caixa? Seção 1.
- O empenhado virou pagamento? Seção 1.
- Como a despesa evoluiu no período? Seção 2.
- Em que tipo de despesa o dinheiro é usado? Seção 3.
- Pagamentos concentrados no fim do período? Seção 4.
        """
    )

st.markdown('<a id="despesas-s1"></a>', unsafe_allow_html=True)
st.subheader("1. Resumo")
df_kpi = run_query(*despesas.kpis_despesa(flt))
df_ef = run_query(*despesas.eficiencia_execucao(flt))
vl_emp = vl_liq = vl_pago = None
pct_liq_emp = pct_pago_emp = pct_pago_liq = None
if not df_kpi.empty:
    d = df_kpi.iloc[0]
    vl_emp = float(d["vl_empenhado"])
    vl_liq = float(d["vl_liquidado"])
    vl_pago = float(d["vl_pago"])
if not df_ef.empty:
    e = df_ef.iloc[0]
    pct_liq_emp = (
        float(e["pct_liquidado_sobre_empenhado"])
        if e["pct_liquidado_sobre_empenhado"] is not None
        else None
    )
    pct_pago_emp = (
        float(e["pct_pago_sobre_empenhado"])
        if e["pct_pago_sobre_empenhado"] is not None
        else None
    )
    pct_pago_liq = (
        float(e["pct_pago_sobre_liquidado"])
        if e["pct_pago_sobre_liquidado"] is not None
        else None
    )

_, h_emp = termo("empenho")
_, h_liq = termo("liquidacao")
_, h_pag = termo("pagamento")
kpi.kpi_row(
    [
        kpi.kpi_brl_compact("Empenhado", vl_emp, h_emp),
        kpi.kpi_brl_compact("Liquidado", vl_liq, h_liq),
        kpi.kpi_brl_compact("Pago (no período)", vl_pago, h_pag),
    ]
)
if (
    pct_liq_emp is not None
    or pct_pago_emp is not None
    or pct_pago_liq is not None
):
    kpi.kpi_row(
        [
            kpi.kpi_pct(
                "% liquidado / empenhado",
                pct_liq_emp,
                "Quanto do empenhado já foi reconhecido (liquidação).",
            ),
            kpi.kpi_pct(
                "% pago / empenhado",
                pct_pago_emp,
                "Quanto do empenhado já virou pagamento efetivo.",
            ),
            kpi.kpi_pct(
                "% pago / liquidado",
                pct_pago_liq,
                "Quanto do liquidado já foi pago.",
            ),
        ]
    )

st.divider()
st.markdown('<a id="despesas-s2"></a>', unsafe_allow_html=True)
st.subheader("2. Evolução mensal")
st.caption(
    "Três linhas: empenho (compromisso), liquidação (serviço/bem entregue) e "
    "pagamento (dinheiro que saiu)."
)
df_serie = run_query(*despesas.serie_mensal_despesa(flt))
if not df_serie.empty:
    df_serie["periodo"] = (
        df_serie["sg_mes"] + "/" + df_serie["nu_ano"].astype(str)
    )
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
        descricao="Acompanha empenho, liquidação e pagamento ao longo do "
        "tempo. Meses com valor zero em alguma série não aparecem no "
        "gráfico log.",
        x_label="Período",
        y_log_scale=True,
    )
else:
    st.info("Sem série mensal para o filtro atual.")

st.divider()
st.markdown('<a id="despesas-s4"></a>', unsafe_allow_html=True)
st.subheader("3. Naturezas que mais consomem recursos")
st.caption(
    "Natureza da despesa = tipo econômico do gasto, não o nome da escola ou hospital."
)
df_nat = run_query(*despesas.ranking_naturezas_despesa(flt))
if not df_nat.empty:
    df_nat["label"] = df_nat["ds_natureza_despesa"].apply(
        limpar_rotulo_natureza_despesa
    )
    charts.bar_horizontal(
        df_nat.head(15),
        y="label",
        x="vl_pago",
        titulo="Principais naturezas de despesa (valor pago)",
        legenda="Barras: total pago por natureza no recorte",
        descricao="Ranking das categorias econômicas que mais receberam "
        "pagamento.",
    )
else:
    st.info("Sem naturezas de despesa no recorte.")

st.divider()
st.markdown('<a id="despesas-s5"></a>', unsafe_allow_html=True)
st.subheader("4. Tendência de pagamentos")
st.caption("Picos no fim do período podem indicar concentração de pagamentos.")
df_acc = run_query(*despesas.aceleracao_gastos(flt))
if not df_acc.empty and len(df_acc) > 1:
    df_acc["periodo"] = df_acc["sg_mes"] + "/" + df_acc["nu_ano"].astype(str)
    charts.line_series(
        df_acc,
        x="periodo",
        y_cols=["vl_pago"],
        labels={"vl_pago": "Pagamentos"},
        titulo="Pagamentos mês a mês",
        legenda="Série de pagamentos mensais",
        descricao="Útil para ver sazonalidade e fechamentos de exercício.",
        x_label="Período",
        y_log_scale=True,
    )
else:
    st.info("Sem série suficiente de pagamentos para esta análise.")
