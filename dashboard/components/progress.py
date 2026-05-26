"""Indicadores de progresso (shell)."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.formatting import fmt_brl, fmt_pct


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
    st.caption(
        f"{fmt_brl(realizado)} de {fmt_brl(previsto)} "
        f"({fmt_pct(pct_val)}) — {ano}"
    )
    if pct_val < 60.0:
        st.error("Atenção: realização bem abaixo do previsto.")
    elif pct_val < 90.0:
        st.warning("Realização abaixo do previsto em parte relevante da meta.")
    else:
        st.success("Realização próxima ou acima do esperado para a meta.")
