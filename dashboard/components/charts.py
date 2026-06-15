"""Componentes de gráficos Plotly com título, legenda e descrição."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

_PLOTLY_CONFIG: dict = {
    "displayModeBar": False,
    "scrollZoom": False,
}


def render_chart(
    fig: go.Figure,
    *,
    titulo: str,
    legenda: str,
    descricao: str,
) -> None:
    st.subheader(titulo)
    fig.update_layout(dragmode=False)
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    st.plotly_chart(
        fig,
        width="stretch",
        config=_PLOTLY_CONFIG,
    )
    st.caption(f"Legenda: {legenda}")
    st.markdown(f":grey[{descricao}]")


def line_series(
    df: pd.DataFrame,
    x: str,
    y_cols: list[str],
    labels: dict[str, str],
    *,
    titulo: str,
    legenda: str,
    descricao: str,
    x_label: str = "Período",
) -> None:
    if df.empty:
        st.info("Sem dados para o período/filtros selecionados.")
        return
    fig = go.Figure()
    for col in y_cols:
        fig.add_trace(
            go.Scatter(
                x=df[x],
                y=df[col],
                mode="lines+markers",
                name=labels.get(col, col),
            )
        )
    fig.update_layout(
        xaxis_title=x_label,
        yaxis_title="Valor (R$)",
        legend_title_text="Série",
        hovermode="x unified",
    )
    render_chart(fig, titulo=titulo, legenda=legenda, descricao=descricao)


def bar_horizontal(
    df: pd.DataFrame,
    y: str,
    x: str,
    *,
    titulo: str,
    legenda: str,
    descricao: str,
    color: str | None = None,
) -> None:
    if df.empty:
        st.info("Sem dados para exibir.")
        return
    fig = px.bar(
        df,
        y=y,
        x=x,
        orientation="h",
        color=color,
        labels={x: "Valor (R$)", y: ""},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    render_chart(fig, titulo=titulo, legenda=legenda, descricao=descricao)


def pareto_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    y2: str,
    *,
    titulo: str,
    legenda: str,
    descricao: str,
) -> None:
    if df.empty:
        st.info("Sem dados para exibir.")
        return
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df[x], y=df[y], name="Valor pago"))
    fig.add_trace(
        go.Scatter(
            x=df[x],
            y=df[y2],
            name="% acumulado",
            yaxis="y2",
            mode="lines+markers",
        )
    )
    fig.update_layout(
        yaxis=dict(title="Valor pago (R$)"),
        yaxis2=dict(
            title="% acumulado", overlaying="y", side="right", range=[0, 105]
        ),
        legend_title_text="Série",
    )
    render_chart(fig, titulo=titulo, legenda=legenda, descricao=descricao)


def grouped_bar(
    df: pd.DataFrame,
    x: str,
    y_cols: list[str],
    labels: dict[str, str],
    *,
    titulo: str,
    legenda: str,
    descricao: str,
) -> None:
    if df.empty:
        st.info("Sem dados para exibir.")
        return
    fig = go.Figure()
    for col in y_cols:
        fig.add_trace(go.Bar(name=labels.get(col, col), x=df[x], y=df[col]))
    fig.update_layout(
        barmode="group", yaxis_title="Valor (R$)", legend_title_text="Métrica"
    )
    render_chart(fig, titulo=titulo, legenda=legenda, descricao=descricao)
