from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import charts, filters, kpi, tables
from dashboard.queries import fornecedores
from dashboard.utils.db import run_query
from dashboard.utils.formatting import fmt_pct
from dashboard.utils.glossary import termo

st.set_page_config(page_title="Fornecedores", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Fornecedores")
st.caption(
    "Veja quais empresas e pessoas mais recebem dinheiro público e se há "
    "concentração de pagamentos."
)


with st.expander("O que você quer descobrir?", expanded=False):
    st.markdown(
        """
- **Quem mais recebeu?** Seção 1.
- **Há concentração em poucos fornecedores?** Seção 2.
- **Como evoluiu um fornecedor específico?** Seção 3.
        """
    )

df_rank = run_query(*fornecedores.ranking_fornecedores(flt))

st.markdown('<a id="fornecedores-s1"></a>', unsafe_allow_html=True)
st.subheader("1. Resumo")
if not df_rank.empty:
    top = df_rank.iloc[0]
    nm = str(top["nm_fornecedor"]).strip() if top["nm_fornecedor"] else ""
    if not nm:
        nm = str(top["cd_cpf_cnpj"])
    pct_top = float(top["pct_total"]) if top["pct_total"] is not None else None
    vl_top = float(top["vl_pago"])
    total_geral = None
    if pct_top is not None and pct_top > 0:
        total_geral = vl_top / (pct_top / 100.0)
    if total_geral is None:
        total_geral = float(df_rank["vl_pago"].sum())
    pct_cinco = sum(
        float(r["pct_total"])
        for _, r in df_rank.head(5).iterrows()
        if r["pct_total"] is not None
    )
    _, h_for = termo("fornecedor")
    kpi.kpi_row(
        [
            kpi.kpi_text(
                "Maior fornecedor (nome)",
                nm[:60] + ("…" if len(nm) > 60 else ""),
                h_for,
            ),
            kpi.kpi_brl_compact(
                "Total pago (base do ranking)",
                total_geral,
                "Soma de todos os fornecedores na base do ranking (até o "
                "limite da consulta).",
            ),
            kpi.kpi_text(
                "% nos 5 maiores",
                fmt_pct(pct_cinco),
                "Parcela do total pago concentrada nos cinco primeiros "
                "do ranking.",
            ),
        ]
    )
else:
    st.info("Sem pagamentos a fornecedores no recorte dos filtros.")

st.divider()
st.markdown('<a id="fornecedores-s2"></a>', unsafe_allow_html=True)
st.subheader("2. Concentração de pagamentos")
if not df_rank.empty:
    df_rank["label"] = df_rank["nm_fornecedor"].fillna(df_rank["cd_cpf_cnpj"])
    charts.bar_horizontal(
        df_rank.head(20),
        y="label",
        x="vl_pago",
        titulo="Fornecedores que mais receberam pagamentos (Top 20)",
        legenda="Valor total pago no período",
        descricao=(
            "Identifica concentração de recursos em poucos fornecedores."
        ),
    )
    tables.render_table(
        df_rank[
            [
                "cd_cpf_cnpj",
                "nm_fornecedor",
                "vl_pago",
                "qtd_empenhos",
                "pct_total",
            ]
        ],
        titulo="Ranking completo",
        column_config={
            "cd_cpf_cnpj": st.column_config.TextColumn("CPF/CNPJ"),
            "nm_fornecedor": st.column_config.TextColumn("Fornecedor"),
            "vl_pago": st.column_config.NumberColumn(
                "Valor pago",
                format="R$ %.2f",
            ),
            "qtd_empenhos": st.column_config.NumberColumn(
                "Qtd. empenhos",
                format="%d",
            ),
            "pct_total": st.column_config.NumberColumn(
                "Participação (%)",
                format="%.2f%%",
            ),
        },
    )
else:
    st.info("Sem dados de fornecedores para gráficos.")

st.divider()
st.markdown('<a id="fornecedores-s3"></a>', unsafe_allow_html=True)
st.subheader("3. Evolução por fornecedor")
if not df_rank.empty:
    labels = {
        r["sk_fornecedor"]: f"{r['nm_fornecedor']} ({r['cd_cpf_cnpj']})"
        for _, r in df_rank.iterrows()
    }
    sk_sel = st.selectbox(
        "Selecione o fornecedor",
        options=list(labels.keys()),
        format_func=lambda k: labels[k],
    )
    if sk_sel:
        df_evo = run_query(*fornecedores.evolucao_fornecedor(flt, sk_sel))
        if not df_evo.empty:
            df_evo["periodo"] = (
                df_evo["sg_mes"] + "/" + df_evo["nu_ano"].astype(str)
            )
            st.metric(
                "Empenhos distintos (período)",
                int(df_evo["qtd_empenhos"].sum()),
            )
            charts.line_series(
                df_evo,
                x="periodo",
                y_cols=["vl_pago"],
                labels={"vl_pago": "Pago"},
                titulo="Evolução temporal de pagamentos",
                legenda="Pagamentos mensais ao fornecedor selecionado "
                "(eixo Y em escala logarítmica)",
                x_label="Período",
                y_log_scale=True,
            )
        else:
            st.info("Sem evolução mensal para este fornecedor no recorte.")
else:
    st.info("Sem fornecedores no ranking para explorar.")
