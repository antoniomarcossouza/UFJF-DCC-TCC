"""Caixas de narrativa na UI (shell)."""

from __future__ import annotations

from typing import Literal

import streamlit as st


def insight_box(
    text: str,
    tone: Literal["info", "success", "warning"] = "info",
) -> None:
    if tone == "success":
        st.success(text)
    elif tone == "warning":
        st.warning(text)
    else:
        st.info(text)


def insight_bullets(items: list[str]) -> None:
    st.markdown("\n".join(f"- {item}" for item in items))
