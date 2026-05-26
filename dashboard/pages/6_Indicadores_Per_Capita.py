"""Página: Indicadores Per Capita."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import disclaimers, filters, kpi
from dashboard.queries import per_capita
from dashboard.utils.config import (
    POPULACAO_PATH,
    get_populacao_ano,
    load_populacao,
)
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_brl, fmt_int
from dashboard.utils.safe_math import divide

st.set_page_config(page_title="Indicadores Per Capita", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.header("Indicadores Per Capita")
disclaimers.render_data_coverage()

ano_ref = flt.anos[0] if flt.anos else None
pop_map = load_populacao()
populacao = get_populacao_ano(ano_ref) if ano_ref else None

if not populacao:
    st.warning(
        f"População não configurada para o ano selecionado. "
        f"Edite `{POPULACAO_PATH}` com dados do IBGE "
        f"(seção `[populacao]`, ex.: `2026 = 75000`)."
    )
    st.code(
        "[populacao]\n2026 = 0  # substitua pelo valor IBGE\n",
        language="toml",
    )
    st.stop()

st.info(f"População utilizada ({ano_ref}): {fmt_int(populacao)} habitantes.")

df_tot = run_query(*per_capita.totais_per_capita(flt))
if df_tot.empty:
    st.stop()

t = df_tot.iloc[0]
arrec = float(t["vl_arrecadada"])
pago = float(t["vl_pago"])
arrec_pc = divide(arrec, populacao)
pago_pc = divide(pago, populacao)

kpi.kpi_row(
    [
        kpi.kpi_brl(
            "Arrecadação per capita",
            arrec_pc,
            "Arrecadação acumulada / população",
        ),
        kpi.kpi_brl(
            "Despesa paga per capita", pago_pc, "Pagamentos / população"
        ),
    ]
)

st.subheader("Investimento setorial per capita (função)")
setores = [
    ("10", "Saúde"),
    ("12", "Educação"),
    ("15", "Urbanismo (infraestrutura)"),
]
cols = st.columns(len(setores))
for col, (cd, nome) in zip(cols, setores, strict=True):
    df_set = run_query(*per_capita.gasto_por_funcao(flt, cd))
    vl = float(df_set.iloc[0]["vl_pago"]) if not df_set.empty else 0.0
    pc = divide(vl, populacao)
    col.metric(
        f"{nome} (função {cd})", fmt_brl(pc), help=f"Total pago: {fmt_brl(vl)}"
    )

st.caption(
    "Funções conforme classificação funcional (código posicional em cd_funcional_pragmatica). "
    "Urbanismo usado como proxy de infraestrutura (função 15)."
)
