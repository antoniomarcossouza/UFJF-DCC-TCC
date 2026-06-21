"""Nomes legíveis para códigos de função orçamentária (functional core)."""

from __future__ import annotations

FUNCAO_NOME: dict[str, str] = {
    "01": "Legislativa",
    "02": "Judiciária",
    "03": "Essencial à Justiça",
    "04": "Administração",
    "05": "Defesa Nacional",
    "06": "Segurança Pública",
    "07": "Relações Exteriores",
    "08": "Assistência Social",
    "09": "Previdência Social",
    "10": "Saúde",
    "11": "Trabalho",
    "12": "Educação",
    "13": "Cultura",
    "14": "Direitos da Cidadania",
    "15": "Urbanismo",
    "16": "Habitação",
    "17": "Saneamento",
    "18": "Gestão Ambiental",
    "19": "Ciência e Tecnologia",
    "20": "Agricultura",
    "21": "Organização Agrária",
    "22": "Indústria",
    "23": "Comércio e Serviços",
    "24": "Comunicações",
    "25": "Energia",
    "26": "Transporte",
    "27": "Desporto e Lazer",
    "28": "Encargos Especiais",
    "99": "Reserva de Contingência",
}


def nome_funcao(cd_funcao: str) -> str:
    """Retorna nome legível ou 'Função {cd}' se desconhecido."""
    s = cd_funcao.strip()
    if not s:
        msg = "cd_funcao não pode ser vazio"
        raise ValueError(msg)
    head = "".join(c for c in s if c.isdigit())
    if len(head) >= 2:
        key = head[:2]
    elif len(head) == 1:
        key = head.zfill(2)
    else:
        msg = "cd_funcao sem dígitos reconhecíveis"
        raise ValueError(msg)
    return FUNCAO_NOME.get(key, f"Função {key}")
