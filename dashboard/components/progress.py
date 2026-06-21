"""Indicadores de progresso (shell)."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.formatting import fmt_brl_compact, fmt_pct


def previsto_realizado_bar(
    realizado: float,
    previsto: float,
    *,
    ano: int | str,
) -> None:
    """Barra de progresso previsto vs realizado com alertas por faixa."""
    if previsto <= 0:
        st.info("Previsão atualizada zero ou ausente para o recorte.")
        return
    ratio = min(1.0, max(0.0, realizado / previsto))
    pct_val = 100.0 * realizado / previsto
    st.progress(ratio)
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"**Realizado**  \n{fmt_brl_compact(realizado)}")
    c2.markdown(f"**Previsto**  \n{fmt_brl_compact(previsto)}")
    c3.markdown(f"**Execução**  \n{fmt_pct(pct_val)}")
