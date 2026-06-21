"""Página: Glossário de termos orçamentários."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import glossary as glossary_ui

st.set_page_config(page_title="Glossário", layout="wide")

st.title("Glossário")
st.caption(
    "Termos orçamentários usados neste painel, explicados em linguagem "
    "simples."
)

glossary_ui.render_glossary()
