from pathlib import Path

import dagster as dg
from dagster.components import definitions


class LocalFSResource(dg.ConfigurableResource):
    base_path: str

    def save_bytes(
        self,
        content: bytes,
        directory: str,
        filename: str,
    ) -> None:
        """Salva bytes no filesystem local."""
        filepath = Path(self.base_path) / directory / filename
        filepath.parent.mkdir(exist_ok=True, parents=True)

        with filepath.open("wb") as f:
            f.write(content)

        return filepath

    def glob(self, directory: str, pattern: str = "*"):
        """
        Retorna uma lista de Paths dentro de
        `directory` que batem com `pattern`.
        """
        dir_path = Path(self.base_path) / directory
        if not dir_path.exists():
            return []
        return list(dir_path.glob(pattern))


@definitions
def defs():
    return dg.Definitions(
        resources={
            "fs": LocalFSResource(
                base_path=str((Path.cwd() / "data").resolve())
            )
        },
    )
