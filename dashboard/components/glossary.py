"""Glossário na UI (shell)."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.glossary import GLOSSARY, termo


def glossary_expander(*, titulo: str = "Glossário de termos") -> None:
    """Lista todos os termos com descrição simples."""
    with st.expander(titulo, expanded=False):
        for key in GLOSSARY:
            label, desc = termo(key)
            st.markdown(f"**{label}**")
            st.caption(desc)


def term_with_tooltip(key: str) -> None:
    """Rótulo + texto auxiliar (sem tooltip nativo em markdown)."""
    label, desc = termo(key)
    st.markdown(f"**{label}**")
    st.caption(desc)
