"""Glossário de termos orçamentários (functional core)."""

from __future__ import annotations

GLOSSARY: dict[str, tuple[str, str]] = {
    "receita_corrente": (
        "Receita corrente",
        (
            "Dinheiro que entra de forma regular "
            "(impostos, taxas, transferências). "
            "Usado para o dia a dia da prefeitura."
        ),
    ),
    "receita_tributaria": (
        "Receita tributária",
        "Tributos cobrados pela própria prefeitura: IPTU, ISS, ITBI, taxas.",
    ),
    "transferencias_correntes": (
        "Transferências correntes",
        "Recursos recebidos do governo federal e estadual (FPM, FUNDEB, "
        "ICMS partilhado) para custeio.",
    ),
    "receita_realizada": (
        "Receita realizada",
        "Valor efetivamente arrecadado no período (entrou no caixa "
        "orçamentário).",
    ),
    "previsao_inicial": (
        "Previsão inicial",
        "Primeira estimativa de quanto o município esperava arrecadar no ano.",
    ),
    "previsao_atualizada": (
        "Previsão atualizada",
        "Expectativa de arrecadação já revisada durante o ano (créditos "
        "adicionais, reestimativas).",
    ),
    "dotacao": (
        "Dotação",
        "No contexto de despesa: limite autorizado para gastar. Em receita, "
        "o portal foca em previsão e realização.",
    ),
    "fpm": (
        "FPM",
        "Fundo de Participação dos Municípios: parcela federal redistribuída "
        "a todos os municípios.",
    ),
    "fundeb": (
        "FUNDEB",
        (
            "Fundo de Manutenção e Desenvolvimento da Educação Básica; "
            "financia ensino básico com contribuições de estados, "
            "municípios e União."
        ),
    ),
    "icms": (
        "ICMS",
        "Imposto estadual sobre circulação de mercadorias; municípios "
        "recebem cota-parte conforme regras de partilha.",
    ),
    "deducao": (
        "Dedução",
        "Valores que reduzem a receita bruta (ex.: contribuição ao FUNDEB, "
        "restituições).",
    ),
}


def termo(key: str) -> tuple[str, str]:
    """Retorna (rótulo curto, descrição em linguagem simples)."""
    if key not in GLOSSARY:
        msg = f"Chave de glossário desconhecida: {key!r}"
        raise KeyError(msg)
    return GLOSSARY[key]
