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
    "empenho": (
        "Empenho",
        "Compromisso formal de gastar: reserva o valor para uma despesa "
        "contratada ou autorizada, antes do pagamento.",
    ),
    "liquidacao": (
        "Liquidação",
        "Reconhecimento de que o serviço foi prestado ou o bem entregue; "
        "confirma o valor a pagar após o empenho.",
    ),
    "pagamento": (
        "Pagamento",
        "Saída efetiva do dinheiro do caixa (ordem bancária, transferência) "
        "para o credor.",
    ),
    "funcao": (
        "Função",
        "Grande área de governo (saúde, educação, transporte) definida por "
        "código numérico no orçamento.",
    ),
    "subfuncao": (
        "Subfunção",
        "Detalhe dentro da função (ex.: ensino fundamental dentro de "
        "educação), também identificada por código.",
    ),
    "natureza_despesa": (
        "Natureza da despesa",
        "Classificação econômica do gasto: pessoal, material, serviços, "
        "obras etc.",
    ),
    "unidade_administrativa": (
        "Unidade administrativa",
        "Órgão ou secretaria responsável por executar parte do orçamento "
        "(empenhos e pagamentos).",
    ),
    "fornecedor": (
        "Fornecedor",
        "Pessoa física ou jurídica que recebe pagamento da prefeitura por "
        "bens ou serviços.",
    ),
    "pareto": (
        "Curva de Pareto",
        "Princípio de Pareto (80/20): em muitos casos, poucos itens somam "
        "a maior parte do total. Aqui, cada barra é o valor pago a um "
        "fornecedor (do maior para o menor); a linha mostra o percentual "
        "acumulado. Se a linha sobe muito rápido à esquerda, poucos "
        "credores concentram quase todo o pagamento.",
    ),
    "saldo_fiscal": (
        "Saldo fiscal (recorte)",
        "Diferença entre arrecadação e pagamentos no período filtrado; "
        "positivo indica que entradas cobriram saídas consolidadas.",
    ),
    "orcamento_autorizado": (
        "Orçamento autorizado",
        "Limite legal de despesa (dotação) aprovado na lei orçamentária. "
        "Não está disponível neste painel; usamos empenho/liquidação/pago.",
    ),
}


def termo(key: str) -> tuple[str, str]:
    """Retorna (rótulo curto, descrição em linguagem simples)."""
    if key not in GLOSSARY:
        msg = f"Chave de glossário desconhecida: {key!r}"
        raise KeyError(msg)
    return GLOSSARY[key]
