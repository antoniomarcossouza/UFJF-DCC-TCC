"""Classificação MCASP de natureza de receita por prefixo (functional core)."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Literal

OrigemReceita = Literal[
    "impostos_proprios",
    "transf_federais",
    "transf_estaduais",
    "outras_transf",
    "outras_correntes",
    "capital",
    "intra",
    "deducoes",
]

ORIGEM_LABEL: dict[str, str] = {
    "impostos_proprios": "Impostos e taxas da própria prefeitura",
    "transf_federais": (
        "Transferências da União (FPM, FUNDEB federal, SUS etc.)"
    ),
    "transf_estaduais": (
        "Transferências estaduais (ICMS partilhado, IPVA etc.)"
    ),
    "outras_transf": "Outras transferências de governos",
    "outras_correntes": "Outras receitas correntes",
    "capital": "Receita de capital",
    "intra": "Operações intra-orçamentárias",
    "deducoes": "Deduções e restituições (reduzem a receita)",
}

ORIGEM_DESCRICAO: dict[str, str] = {
    "impostos_proprios": (
        "Tributos e taxas cobrados pelo município, como IPTU, ISS e taxas."
    ),
    "transf_federais": (
        "Recursos vindos do governo federal: FPM, parcelas do FUNDEB, "
        "repasses de convênios e programas federais."
    ),
    "transf_estaduais": (
        "Recursos vindos do estado: cota-parte do ICMS, IPVA e demais "
        "repasses estaduais."
    ),
    "outras_transf": (
        "Demais transferências correntes entre esferas que não se enquadram "
        "nas categorias federal ou estadual principal."
    ),
    "outras_correntes": (
        "Receitas correntes que não são impostos próprios nem as principais "
        "transferências classificadas acima."
    ),
    "capital": (
        "Receitas destinadas a investimentos e amortização da dívida, "
        "conforme classificação orçamentária."
    ),
    "intra": (
        "Movimentações entre contas do próprio orçamento, sem entrada "
        "externa de recursos."
    ),
    "deducoes": (
        "Valores que reduzem a receita bruta, como deduções legais e "
        "restituições (códigos contábeis da STN em 9)."
    ),
}


def classify_origem(cd: str) -> OrigemReceita:
    """Classifica código de natureza de receita (MCASP) em origem agregada."""
    if cd is None or not isinstance(cd, str):
        msg = "cd_natureza_receita deve ser string não vazia"
        raise TypeError(msg)
    s = cd.strip()
    if not s:
        msg = "cd_natureza_receita não pode ser vazio"
        raise ValueError(msg)

    if s.startswith("171"):
        return "transf_federais"
    if s.startswith("172"):
        return "transf_estaduais"
    if s.startswith(("173", "174", "175")):
        return "outras_transf"
    if s.startswith("17"):
        return "outras_transf"
    if s.startswith("11"):
        return "impostos_proprios"
    if s[0] == "9":
        return "deducoes"
    if s[0] in ("7", "8"):
        return "intra"
    if s[0] == "2":
        return "capital"
    if s[0] == "1":
        return "outras_correntes"
    return "outras_correntes"


def _pad_codigo_mcasp(prefixo: str) -> str:
    """Expande prefixo (ex.: 3 dígitos SQL) para 8 caracteres MCASP."""
    s = prefixo.strip()
    if len(s) >= 8:
        return s[:8]
    return (s + "0" * 8)[:8]


def aggregate_vl_por_origem(
    prefixos_valores: Iterable[tuple[str, float]],
) -> dict[str, float]:
    """Soma vl por origem a partir de prefixo SQL (3 dígitos)."""
    acc: dict[str, float] = defaultdict(float)
    for p3, vl in prefixos_valores:
        key = classify_origem(_pad_codigo_mcasp(p3))
        acc[key] += float(vl)
    return dict(acc)
