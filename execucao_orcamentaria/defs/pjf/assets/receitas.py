from datetime import datetime

import dagster as dg
from pathlib import Path
import requests
from dagster.components import definitions


def fetch_xls(url: str) -> bytes:
    """Baixa XLS bruto de uma URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.content


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


receita_mensal_prevista_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%y",
    end_offset=1,
)

receita_mensal_comparativa_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 * *",
    fmt="%y%m",
)


@dg.asset(
    partitions_def=receita_mensal_prevista_partition,
    kinds={"python", "pandas", "duckdb"},
    group_name="receitas",
    description="Receita Mensal Prevista",
)
def receita_mensal_prevista(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
) -> dg.MaterializeResult:
    """
    Baixa o XLS de receita mensal prevista,
    lê com pandas e salva em uma tabela DuckDB.
    Suporta backfill anual baseado em partições.
    """
    year = int(context.partition_key)

    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/previsao/arquivos/xls/{year}.xls"
    content = fetch_xls(url)

    fs.save_bytes(
        content=content,
        directory="pjf_receita_mensal_prevista",
        filename=url.split("/")[-1],
    )

    return dg.MaterializeResult()


@dg.asset(
    partitions_def=receita_mensal_comparativa_partition,
    kinds={"python", "pandas", "duckdb"},
    group_name="receitas",
    description="Receita Mensal Comparativa",
)
def receita_mensal_comparativa(
    context: dg.AssetExecutionContext,
    fs: LocalFSResource,
) -> dg.MaterializeResult:
    year_month = context.partition_key

    url = f"https://www.pjf.mg.gov.br/transparencia/receitas/mensal/comparativo/arquivos/xls/{year_month}.xls"
    content = fetch_xls(url)

    fs.save_bytes(
        content=content,
        directory="pjf_receita_mensal_comparativa",
        filename=url.split("/")[-1],
    )

    return dg.MaterializeResult()


@definitions
def defs():
    return dg.Definitions(
        assets=[
            receita_mensal_prevista,
            receita_mensal_comparativa,
        ],
        resources={
            "fs": LocalFSResource(
                base_path="/home/antonio/projects/ufjf/ufjf-tcc/data"
            )
        },
    )
