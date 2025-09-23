from collections.abc import Sequence
from functools import cache
from typing import Optional

import dagster as dg
from dagster.components import definitions
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
)
from dagster_dbt.asset_utils import DBT_DEFAULT_SELECT

from execucao_orcamentaria.defs.dbt.resources import (
    execucao_orcamentaria_dbt_project,
)

INCREMENTAL_SELECTOR = "config.materialized:incremental"
SNAPSHOT_SELECTOR = "resource_type:snapshot"


@cache
def get_dbt_non_partitioned_models(
    additional_selectors: Optional[Sequence[str]] = None,
):
    dbt_project = execucao_orcamentaria_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=",".join(additional_selectors or [DBT_DEFAULT_SELECT]),
        exclude=" ".join([INCREMENTAL_SELECTOR, SNAPSHOT_SELECTOR]),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_non_partitioned_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource
    ):
        yield from (
            dbt.cli(["build"], context=context)
            .stream()
            .fetch_column_metadata()
            .with_insights()
        )

    return dbt_non_partitioned_models


@definitions
def defs():
    dbt_non_partitioned_models = get_dbt_non_partitioned_models()

    return dg.Definitions(
        assets=[
            dbt_non_partitioned_models,
        ]
    )
