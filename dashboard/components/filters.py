"""Sidebar de filtros globais (session_state + query params na URL)."""

from __future__ import annotations

import math

import pandas as pd
import streamlit as st

from dashboard.queries import dim_options
from dashboard.queries.filters import FilterState
from dashboard.utils.db import run_query
from dashboard.utils.filter_persistence import (
    FILTER_QUERY_PARAMS,
    QP_ANOS,
    QP_FONTES,
    QP_FORNECEDOR_BUSCA,
    QP_FORNECEDORES,
    QP_FUNCOES,
    QP_MESES,
    QP_NAT_DESPESA,
    QP_NAT_RECEITA,
    QP_UNIDADES,
    build_filter_query_params,
    current_filter_query_params,
    read_filter_query_params,
)

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
K_FILTERS_HYDRATED = "_global_filters_hydrated_from_url"

_STATE_BY_QUERY_PARAM = {
    QP_ANOS: K_ANOS,
    QP_MESES: K_MESES,
    QP_UNIDADES: K_UNIDADES,
    QP_FUNCOES: K_FUNCOES,
    QP_NAT_DESPESA: K_NAT_DESPESA,
    QP_FONTES: K_FONTES,
    QP_FORNECEDORES: K_FORNECEDORES,
    QP_NAT_RECEITA: K_NAT_RECEITA,
    QP_FORNECEDOR_BUSCA: K_FORNECEDOR_BUSCA,
}


def _hydrate_from_url() -> None:
    """Carrega filtros da URL na primeira execução da sessão (ex.: após F5)."""
    if st.session_state.get(K_FILTERS_HYDRATED):
        return
    parsed = read_filter_query_params(dict(st.query_params))
    for qp_key, state_key in _STATE_BY_QUERY_PARAM.items():
        if qp_key not in parsed:
            continue
        st.session_state[state_key] = parsed[qp_key]
    st.session_state[K_FILTERS_HYDRATED] = True


def _sync_to_url() -> None:
    """Espelha filtros atuais na URL para sobreviver a refresh."""
    desired = build_filter_query_params(
        anos=st.session_state.get(K_ANOS, []),
        meses=st.session_state.get(K_MESES, []),
        unidades=st.session_state.get(K_UNIDADES, []),
        funcoes=st.session_state.get(K_FUNCOES, []),
        naturezas_despesa=st.session_state.get(K_NAT_DESPESA, []),
        fontes=st.session_state.get(K_FONTES, []),
        fornecedores=st.session_state.get(K_FORNECEDORES, []),
        naturezas_receita=st.session_state.get(K_NAT_RECEITA, []),
        fornecedor_busca=st.session_state.get(K_FORNECEDOR_BUSCA, ""),
    )
    if desired == current_filter_query_params(dict(st.query_params)):
        return
    for key in FILTER_QUERY_PARAMS:
        if key in st.query_params:
            del st.query_params[key]
    for key, value in desired.items():
        st.query_params[key] = value


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
    _hydrate_from_url()

    anos_opts = _options(*dim_options.anos_disponiveis())
    _init_multiselect(K_ANOS, anos_opts)
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
    _sync_to_url()
    return filters


def get_filters() -> FilterState:
    if "filters" in st.session_state:
        return st.session_state["filters"]
    return render_sidebar_filters()
