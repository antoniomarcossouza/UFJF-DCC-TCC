"""Sidebar de filtros globais (persistidos em session_state)."""

from __future__ import annotations

import math

import pandas as pd
import streamlit as st

from dashboard.queries import dim_options
from dashboard.queries.filters import FilterState
from dashboard.utils.db import run_query

# Chaves em st.session_state — compartilhadas entre todas as páginas
K_ANOS = "global_filter_anos"
K_MESES = "global_filter_meses"
K_UNIDADES = "global_filter_unidades"
K_FUNCOES = "global_filter_funcoes"
K_NAT_DESPESA = "global_filter_naturezas_despesa"
K_FONTES = "global_filter_fontes"
K_FORNECEDORES = "global_filter_fornecedores"
K_FORNECEDOR_BUSCA = "global_filter_fornecedor_busca"
K_NAT_RECEITA = "global_filter_naturezas_receita"


def _normalize_key(valor) -> str | int:
    """Converte valores DuckDB/pandas para tipos nativos do Python."""
    if valor is None or (isinstance(valor, float) and math.isnan(valor)):
        raise ValueError("valor nulo")
    if pd.isna(valor):
        raise ValueError("valor nulo")
    if isinstance(valor, (bool,)):
        return str(valor)
    if isinstance(valor, (int,)) and not isinstance(valor, bool):
        return int(valor)
    if hasattr(valor, "item"):
        native = valor.item()
        if isinstance(native, int) and not isinstance(native, bool):
            return int(native)
        return str(native)
    return str(valor)


def _normalize_label(rotulo, key: str | int) -> str:
    if rotulo is None or (isinstance(rotulo, float) and math.isnan(rotulo)):
        return str(key)
    if pd.isna(rotulo):
        return str(key)
    text = str(rotulo).strip()
    return text if text else str(key)


def _options(sql: str, params: list | None = None) -> dict[str | int, str]:
    df = run_query(sql, params or [])
    if df.empty:
        return {}
    result: dict[str | int, str] = {}
    for _, row in df.iterrows():
        try:
            key = _normalize_key(row["valor"])
        except ValueError:
            continue
        if key in result:
            continue
        result[key] = _normalize_label(row["rotulo"], key)
    return result


def _format_label(opts: dict, key: str | int) -> str:
    return opts.get(key, str(key))


def _prune_selection(state_key: str, valid_options: list) -> None:
    """Mantém apenas valores ainda válidos nas opções atuais."""
    selected = st.session_state.get(state_key, [])
    st.session_state[state_key] = [x for x in selected if x in valid_options]


def _init_multiselect(
    state_key: str,
    options: dict[str | int, str],
    *,
    default_first: bool = False,
) -> None:
    if state_key not in st.session_state:
        if default_first and options:
            st.session_state[state_key] = [next(iter(options))]
        else:
            st.session_state[state_key] = []


def render_sidebar_filters() -> FilterState:
    st.sidebar.header("Filtros globais")

    anos_opts = _options(*dim_options.anos_disponiveis())
    _init_multiselect(K_ANOS, anos_opts, default_first=True)
    _prune_selection(K_ANOS, list(anos_opts.keys()))
    st.sidebar.multiselect(
        "Ano",
        options=list(anos_opts.keys()),
        format_func=lambda k, o=anos_opts: _format_label(o, k),
        key=K_ANOS,
    )
    anos_sel: list = st.session_state[K_ANOS]

    meses_opts = _options(*dim_options.meses_disponiveis(tuple(anos_sel)))
    _init_multiselect(K_MESES, meses_opts)
    _prune_selection(K_MESES, list(meses_opts.keys()))
    st.sidebar.multiselect(
        "Mês",
        options=list(meses_opts.keys()),
        format_func=lambda k, o=meses_opts: _format_label(o, k),
        key=K_MESES,
    )

    ua_opts = _options(*dim_options.unidades_administrativas())
    _init_multiselect(K_UNIDADES, ua_opts)
    _prune_selection(K_UNIDADES, list(ua_opts.keys()))
    st.sidebar.multiselect(
        "Unidade administrativa",
        options=list(ua_opts.keys()),
        format_func=lambda k, o=ua_opts: _format_label(o, k),
        key=K_UNIDADES,
    )

    func_opts = _options(*dim_options.funcoes())
    _init_multiselect(K_FUNCOES, func_opts)
    _prune_selection(K_FUNCOES, list(func_opts.keys()))
    st.sidebar.multiselect(
        "Função (código)",
        options=list(func_opts.keys()),
        format_func=lambda k, o=func_opts: _format_label(o, k),
        key=K_FUNCOES,
    )

    nd_opts = _options(*dim_options.naturezas_despesa())
    _init_multiselect(K_NAT_DESPESA, nd_opts)
    _prune_selection(K_NAT_DESPESA, list(nd_opts.keys()))
    st.sidebar.multiselect(
        "Natureza da despesa",
        options=list(nd_opts.keys()),
        format_func=lambda k, o=nd_opts: _format_label(o, k),
        key=K_NAT_DESPESA,
    )

    fr_opts = _options(*dim_options.fontes_recurso())
    _init_multiselect(K_FONTES, fr_opts)
    _prune_selection(K_FONTES, list(fr_opts.keys()))
    st.sidebar.multiselect(
        "Fonte de recurso",
        options=list(fr_opts.keys()),
        format_func=lambda k, o=fr_opts: _format_label(o, k),
        key=K_FONTES,
    )

    if K_FORNECEDOR_BUSCA not in st.session_state:
        st.session_state[K_FORNECEDOR_BUSCA] = ""
    st.sidebar.text_input("Buscar fornecedor", key=K_FORNECEDOR_BUSCA)
    fo_search = st.session_state[K_FORNECEDOR_BUSCA]

    fo_opts = _options(*dim_options.fornecedores())
    fo_filtered = {
        k: v
        for k, v in fo_opts.items()
        if not fo_search or fo_search.lower() in v.lower()
    }
    _init_multiselect(K_FORNECEDORES, fo_filtered)
    _prune_selection(K_FORNECEDORES, list(fo_filtered.keys()))
    st.sidebar.multiselect(
        "Fornecedor",
        options=list(fo_filtered.keys()),
        format_func=lambda k, o=fo_filtered: _format_label(o, k),
        key=K_FORNECEDORES,
    )

    nr_opts = _options(*dim_options.naturezas_receita())
    _init_multiselect(K_NAT_RECEITA, nr_opts)
    _prune_selection(K_NAT_RECEITA, list(nr_opts.keys()))
    st.sidebar.multiselect(
        "Natureza da receita",
        options=list(nr_opts.keys()),
        format_func=lambda k, o=nr_opts: _format_label(o, k),
        key=K_NAT_RECEITA,
    )

    filters = FilterState(
        anos=tuple(int(a) for a in st.session_state[K_ANOS]),
        meses=tuple(int(m) for m in st.session_state[K_MESES]),
        sk_unidades=tuple(str(x) for x in st.session_state[K_UNIDADES]),
        cd_funcoes=tuple(str(x) for x in st.session_state[K_FUNCOES]),
        sk_naturezas_despesa=tuple(
            str(x) for x in st.session_state[K_NAT_DESPESA]
        ),
        sk_fontes=tuple(str(x) for x in st.session_state[K_FONTES]),
        sk_fornecedores=tuple(
            str(x) for x in st.session_state[K_FORNECEDORES]
        ),
        sk_naturezas_receita=tuple(
            str(x) for x in st.session_state[K_NAT_RECEITA]
        ),
    )
    st.session_state["filters"] = filters
    return filters


def get_filters() -> FilterState:
    if "filters" in st.session_state:
        return st.session_state["filters"]
    return render_sidebar_filters()
