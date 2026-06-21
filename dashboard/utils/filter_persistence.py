"""Serialização de filtros globais para query params da URL (sobrevive a F5)."""

from __future__ import annotations

QP_ANOS = "anos"
QP_MESES = "meses"
QP_UNIDADES = "ua"
QP_FUNCOES = "func"
QP_NAT_DESPESA = "nd"
QP_FONTES = "fontes"
QP_FORNECEDORES = "forn"
QP_NAT_RECEITA = "nr"
QP_FORNECEDOR_BUSCA = "fb"

FILTER_QUERY_PARAMS = (
    QP_ANOS,
    QP_MESES,
    QP_UNIDADES,
    QP_FUNCOES,
    QP_NAT_DESPESA,
    QP_FONTES,
    QP_FORNECEDORES,
    QP_NAT_RECEITA,
    QP_FORNECEDOR_BUSCA,
)


def _qp_value(raw: str | list[str] | None) -> str:
    if raw is None:
        return ""
    if isinstance(raw, list):
        return raw[0] if raw else ""
    return raw


def parse_int_list(raw: str | list[str] | None) -> list[int]:
    text = _qp_value(raw)
    if not text:
        return []
    out: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out


def parse_str_list(raw: str | list[str] | None) -> list[str]:
    text = _qp_value(raw)
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def serialize_int_list(values: list | tuple) -> str:
    return ",".join(str(int(v)) for v in values)


def serialize_str_list(values: list | tuple) -> str:
    return ",".join(str(v) for v in values)


def build_filter_query_params(
    *,
    anos: list | tuple,
    meses: list | tuple,
    unidades: list | tuple,
    funcoes: list | tuple,
    naturezas_despesa: list | tuple,
    fontes: list | tuple,
    fornecedores: list | tuple,
    naturezas_receita: list | tuple,
    fornecedor_busca: str,
) -> dict[str, str]:
    params: dict[str, str] = {}
    if anos:
        params[QP_ANOS] = serialize_int_list(anos)
    if meses:
        params[QP_MESES] = serialize_int_list(meses)
    if unidades:
        params[QP_UNIDADES] = serialize_str_list(unidades)
    if funcoes:
        params[QP_FUNCOES] = serialize_str_list(funcoes)
    if naturezas_despesa:
        params[QP_NAT_DESPESA] = serialize_str_list(naturezas_despesa)
    if fontes:
        params[QP_FONTES] = serialize_str_list(fontes)
    if fornecedores:
        params[QP_FORNECEDORES] = serialize_str_list(fornecedores)
    if naturezas_receita:
        params[QP_NAT_RECEITA] = serialize_str_list(naturezas_receita)
    if fornecedor_busca.strip():
        params[QP_FORNECEDOR_BUSCA] = fornecedor_busca.strip()
    return params


def read_filter_query_params(
    query_params: dict[str, str | list[str]],
) -> dict[str, list[int] | list[str] | str]:
    """Lê query params e devolve valores prontos para session_state."""
    out: dict[str, list[int] | list[str] | str] = {}
    if QP_ANOS in query_params:
        out[QP_ANOS] = parse_int_list(query_params[QP_ANOS])
    if QP_MESES in query_params:
        out[QP_MESES] = parse_int_list(query_params[QP_MESES])
    if QP_UNIDADES in query_params:
        out[QP_UNIDADES] = parse_str_list(query_params[QP_UNIDADES])
    if QP_FUNCOES in query_params:
        out[QP_FUNCOES] = parse_str_list(query_params[QP_FUNCOES])
    if QP_NAT_DESPESA in query_params:
        out[QP_NAT_DESPESA] = parse_str_list(query_params[QP_NAT_DESPESA])
    if QP_FONTES in query_params:
        out[QP_FONTES] = parse_str_list(query_params[QP_FONTES])
    if QP_FORNECEDORES in query_params:
        out[QP_FORNECEDORES] = parse_str_list(query_params[QP_FORNECEDORES])
    if QP_NAT_RECEITA in query_params:
        out[QP_NAT_RECEITA] = parse_str_list(query_params[QP_NAT_RECEITA])
    if QP_FORNECEDOR_BUSCA in query_params:
        out[QP_FORNECEDOR_BUSCA] = _qp_value(query_params[QP_FORNECEDOR_BUSCA])
    return out


def current_filter_query_params(
    query_params: dict[str, str | list[str]],
) -> dict[str, str]:
    return {
        key: _qp_value(query_params.get(key))
        for key in FILTER_QUERY_PARAMS
        if key in query_params
    }
