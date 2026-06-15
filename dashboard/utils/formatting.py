"""Formatação de valores para exibição (functional core)."""

from __future__ import annotations

import re
from datetime import date


def fmt_brl(value: float | int | None) -> str:
    if value is None:
        return "—"
    return (
        f"R$ {value:,.2f}".replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    if value is None:
        return "—"
    return f"{value:.{decimals}f}%"


def fmt_date(value: date | None) -> str:
    if value is None:
        return "—"
    return value.strftime("%d/%m/%Y")


def _limpar_prefixo_codigo_descricao(ds: str | None) -> str:
    """Remove prefixo tipo código MCASP antes do traço (receita/despesa)."""
    if ds is None:
        return "—"
    s = str(ds).strip()
    if not s:
        return "—"
    s = re.sub(r"^\d+(\.\d+)?\s*-\s*", "", s)
    s = s.strip()
    return s if s else "—"


def limpar_rotulo_natureza_receita(ds: str | None) -> str:
    """Remove prefixo numérico de ds_natureza_receita (ex.: '161... - ')."""
    return _limpar_prefixo_codigo_descricao(ds)


def limpar_rotulo_natureza_despesa(ds: str | None) -> str:
    """Remove prefixo numérico de ds_natureza_despesa (ex.: '339039... - ')."""
    return _limpar_prefixo_codigo_descricao(ds)


def _brl_compact_body(abs_val: float, decimals: int) -> str:
    """Parte numérica já em pt-BR, sem prefixo R$."""
    if abs_val >= 1_000_000_000:
        n = abs_val / 1_000_000_000
        suf = "bi"
    elif abs_val >= 1_000_000:
        n = abs_val / 1_000_000
        suf = "mi"
    elif abs_val >= 1_000:
        n = abs_val / 1_000
        suf = "mil"
    else:
        raw = f"{abs_val:,.2f}"
        txt = raw.replace(",", "X").replace(".", ",").replace("X", ".")
        return txt
    txt = f"{n:.{decimals}f}".replace(".", ",")
    return f"{txt} {suf}"


def fmt_brl_compact(value: float | int | None, *, decimals: int = 1) -> str:
    """Valor em reais legível (mi / bi / mil) para leigos."""
    if value is None:
        return "—"
    v = float(value)
    neg = v < 0
    body = _brl_compact_body(abs(v), decimals)
    prefix = "R$ -" if neg else "R$ "
    return prefix + body
