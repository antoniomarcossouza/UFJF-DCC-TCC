"""Página: Receitas (visão cidadão)."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import (
    charts,
    disclaimers,
    filters,
    progress,
    tables,
)
from dashboard.components import (
    glossary as glossary_ui,
)
from dashboard.components import (
    narrative as narrative_ui,
)
from dashboard.queries import natureza_classification as nc
from dashboard.queries import receitas
from dashboard.queries.filters import FilterState
from dashboard.utils.db import run_query
from dashboard.utils.formatting import (
    fmt_brl_compact,
    fmt_pct,
    limpar_rotulo_natureza_receita,
)
from dashboard.utils.glossary import termo
from dashboard.utils.narrative import (
    insight_evolucao_anual,
    insight_execucao,
    insight_participacao_transferencias,
    insight_principal_fonte,
)
from dashboard.utils.safe_math import pct


def _rotulo_arrecadado(flt: FilterState) -> str:
    if len(flt.anos) == 1:
        return f"Arrecadado ({flt.anos[0]})"
    if len(flt.anos) > 1:
        return "Arrecadado (período filtrado)"
    return "Arrecadado (todos os anos)"


def _rotulo_periodo_progresso(flt: FilterState) -> int | str:
    if len(flt.anos) == 1:
        return int(flt.anos[0])
    if len(flt.anos) > 1:
        return "período selecionado"
    return "todos os anos"


st.set_page_config(page_title="Receitas", layout="wide")
filters.render_sidebar_filters()
flt = filters.get_filters()

st.title("Receitas")
st.caption(
    "Entenda de onde vem o dinheiro da prefeitura e se a arrecadação está "
    "no esperado em relação à previsão."
)
disclaimers.render_data_coverage()

df_kpi = run_query(*receitas.kpis_execucao_receita(flt))
exec_pct = None
vl_prev = None
vl_arr_ano = None
if not df_kpi.empty:
    r = df_kpi.iloc[0]
    vl_prev = float(r["vl_previsao_atualizada"])
    vl_arr_ano = float(r["vl_arrecadada_ano"])
    exec_pct = pct(vl_arr_ano, vl_prev)

df_pref = run_query(*receitas.arrecadacao_por_origem(flt))
totais_origem: dict[str, float] = {}
if not df_pref.empty:
    pairs = zip(
        df_pref["cd_prefixo3"],
        df_pref["vl_arrecadada"],
        strict=True,
    )
    totais_origem = nc.aggregate_vl_por_origem(list(pairs))
total_para_share = sum(max(0.0, v) for v in totais_origem.values()) or 0.0
pct_fed = pct(totais_origem.get("transf_federais", 0.0), total_para_share)
pct_est = pct(totais_origem.get("transf_estaduais", 0.0), total_para_share)
candidatos = {
    k: v for k, v in totais_origem.items() if k != "deducoes" and v > 0
}
principal_key = (
    max(candidatos, key=lambda k: candidatos[k]) if candidatos else None
)
pct_principal = (
    pct(candidatos[principal_key], total_para_share)
    if principal_key and total_para_share > 0
    else None
)
label_principal = (
    nc.ORIGEM_LABEL.get(principal_key, "—") if principal_key else "—"
)

with st.expander("O que você quer descobrir?", expanded=False):
    st.markdown(
        """
- **Maiores fontes** — seção 2 (gráfico por origem).
- **Quanto vem do governo federal** — cards “Repasse federal” no resumo.
- **Arrecadou o esperado?** — seção 4 (previsto vs realizado).
        """
    )
st.caption(
    "Os números seguem o recorte escolhido nos filtros da barra lateral "
    "(ano, mês e natureza da receita)."
)

st.markdown('<a id="receitas-s1"></a>', unsafe_allow_html=True)
st.subheader("1. Resumo")
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric(
        "Previsão",
        fmt_brl_compact(vl_prev),
        help=termo("previsao_atualizada")[1],
    )
with c2:
    st.metric(
        _rotulo_arrecadado(flt),
        fmt_brl_compact(vl_arr_ano),
        help=termo("receita_realizada")[1],
    )
with c3:
    st.metric(
        "Realizado / Previsão",
        fmt_pct(exec_pct),
        help=termo("previsao_atualizada")[1],
    )
with c4:
    st.metric(
        "Repasse federal",
        fmt_pct(pct_fed),
        help="Parcela do arrecadado classificada como transferências da "
        "União (códigos 171…, ex.: FPM, FUNDEB).",
    )
with c5:
    st.metric(
        "Repasse estadual",
        fmt_pct(pct_est),
        help="Parcela classificada como transferências estaduais "
        "(códigos 172…, ex.: ICMS).",
    )

bullets = [insight_execucao(vl_arr_ano, vl_prev, exec_pct)]
if principal_key:
    bullets.append(insight_principal_fonte(label_principal, pct_principal))
narrative_ui.insight_bullets(bullets)

st.divider()
st.markdown('<a id="receitas-s2"></a>', unsafe_allow_html=True)
st.subheader("2. De onde vem o dinheiro")
st.caption(
    "A maior parte da receita municipal costuma vir de repasses dos "
    "governos federal e estadual, não só de impostos cobrados aqui."
)
fed_v = totais_origem.get("transf_federais", 0.0)
est_v = totais_origem.get("transf_estaduais", 0.0)
out_tr_v = totais_origem.get("outras_transf", 0.0)
st.caption(
    "Repasses no recorte (valores brutos): "
    f"União {fmt_brl_compact(fed_v)}; "
    f"estado {fmt_brl_compact(est_v)}; "
    f"outras transferências {fmt_brl_compact(out_tr_v)}. "
    "No gráfico, repasses da União e do estado aparecem nas barras "
    "correspondentes quando há arrecadação nessas classificações."
)
if totais_origem:
    rows = [
        {"origem": nc.ORIGEM_LABEL[k], "valor": v}
        for k, v in sorted(
            totais_origem.items(), key=lambda x: x[1], reverse=True
        )
        if v != 0
    ]
    df_bar = pd.DataFrame(rows)
    charts.bar_horizontal(
        df_bar,
        y="origem",
        x="valor",
        titulo="Composição por origem (valores no período)",
        legenda="Barras: total arrecadado no recorte dos filtros.",
        descricao=(
            "Origens por regras do código de receita: 171… União, 172… "
            "estado, 17… demais transferências, 11… impostos próprios."
        ),
    )
else:
    st.info("Sem arrecadação no recorte para agrupar por origem.")

narrative_ui.insight_box(
    insight_participacao_transferencias(pct_fed, pct_est),
    tone="info",
)
glossary_ui.glossary_expander()

st.divider()
st.markdown('<a id="receitas-s3"></a>', unsafe_allow_html=True)
st.subheader("3. Evolução histórica")
st.caption(
    "Histórico com todos os anos da base. Filtro de ano da barra lateral "
    "não se aplica aqui; mês e natureza da receita sim. "
    "Duas visões: total por ano e mês a mês."
)
flt_nat_hist = FilterState(
    meses=flt.meses,
    sk_naturezas_receita=flt.sk_naturezas_receita,
)
df_anual = run_query(*receitas.serie_anual_previsto_realizado(flt_nat_hist))
if not df_anual.empty:
    df_anual["ano_str"] = df_anual["nu_ano"].astype(str)
    charts.line_series(
        df_anual,
        x="ano_str",
        y_cols=["vl_arrecadada", "vl_previsto"],
        labels={
            "vl_arrecadada": "Realizado (arrecadado)",
            "vl_previsto": "Previsto (soma mensal)",
        },
        titulo="Por ano: previsto vs realizado",
        legenda="Linhas: arrecadado e soma do previsto mensal por ano.",
        descricao="Compara meta acumulada no ano com o que entrou.",
        x_label="Ano",
    )
    vals = {
        int(r["nu_ano"]): float(r["vl_arrecadada"])
        for _, r in df_anual.iterrows()
    }
    ano_parcial = max(vals) if vals else None
    narrative_ui.insight_box(
        insight_evolucao_anual(vals, ano_parcial=ano_parcial),
        tone="info",
    )
else:
    st.info("Sem série anual para o filtro atual.")

df_mensal = run_query(*receitas.serie_mensal_arrecadacao(flt_nat_hist))
if not df_mensal.empty:
    df_mensal["periodo"] = (
        df_mensal["sg_mes"] + "/" + df_mensal["nu_ano"].astype(str)
    )
    charts.line_series(
        df_mensal,
        x="periodo",
        y_cols=["vl_arrecadada", "vl_previsto"],
        labels={
            "vl_arrecadada": "Realizado (mês)",
            "vl_previsto": "Previsto (mês)",
        },
        titulo="Por mês: previsto vs realizado",
        legenda="Eixo X: mês/ano; linhas: arrecadado e previsto mensal.",
        descricao="Detalhe mês a mês no mesmo recorte de mês/natureza.",
        x_label="Período",
    )
else:
    st.info("Sem série mensal para o filtro atual.")

st.divider()
st.markdown('<a id="receitas-s4"></a>', unsafe_allow_html=True)
st.subheader("4. Previsto vs realizado")
pi_label, pi_desc = termo("previsao_atualizada")
rr_label, rr_desc = termo("receita_realizada")
st.markdown(f"**{pi_label}** — {pi_desc}  \n**{rr_label}** — {rr_desc}")
if vl_prev is not None and vl_arr_ano is not None:
    progress.previsto_realizado_bar(
        vl_arr_ano,
        vl_prev,
        ano=_rotulo_periodo_progresso(flt),
    )
else:
    st.info(
        "Indicador indisponível (dados de previsão ou realizado ausentes)."
    )

df_abaixo = run_query(*receitas.receitas_abaixo_previsto(flt))
if not df_abaixo.empty:
    st.caption(
        "Naturezas com realização abaixo de 80% da previsão atualizada "
        "(tabela completa):"
    )
    df_show = df_abaixo.copy()
    df_show["ds_natureza_receita"] = df_show["ds_natureza_receita"].apply(
        limpar_rotulo_natureza_receita
    )
    df_show["vl_arrecadada_ano"] = df_show["vl_arrecadada_ano"].apply(
        lambda x: fmt_brl_compact(float(x))
    )
    df_show["vl_previsao_atualizada"] = df_show[
        "vl_previsao_atualizada"
    ].apply(lambda x: fmt_brl_compact(float(x)))
    df_show["pct_realizacao"] = df_show["pct_realizacao"].apply(
        lambda x: fmt_pct(float(x)) if x is not None else "—"
    )
    tables.render_table(
        df_show,
        descricao="Ordenado do menor para o maior percentual de realização.",
    )
else:
    st.info("Nenhuma natureza abaixo de 80% da previsão neste recorte.")
