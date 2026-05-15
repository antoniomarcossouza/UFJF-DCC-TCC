"""Carregamento de configuração externa (shell)."""

from __future__ import annotations

import tomllib
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
POPULACAO_PATH = CONFIG_DIR / "populacao.toml"


def load_populacao() -> dict[int, int]:
    """Retorna mapa ano -> habitantes; vazio se arquivo ausente."""
    if not POPULACAO_PATH.exists():
        return {}
    data = tomllib.loads(POPULACAO_PATH.read_text(encoding="utf-8"))
    raw = data.get("populacao", {})
    return {int(k): int(v) for k, v in raw.items() if int(v) > 0}


def get_populacao_ano(ano: int) -> int | None:
    pop = load_populacao()
    val = pop.get(ano)
    return val if val and val > 0 else None
