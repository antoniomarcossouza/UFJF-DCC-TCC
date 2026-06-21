"""Glossário na UI (shell)."""

from __future__ import annotations

import streamlit as st

from dashboard.utils.glossary import GLOSSARY, termo


def render_glossary() -> None:
    """Lista todos os termos com descrição simples."""
    for key in GLOSSARY:
        label, desc = termo(key)
        st.markdown(f"**{label}**")
        st.caption(desc)
