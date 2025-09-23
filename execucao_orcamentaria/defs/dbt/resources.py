from functools import cache
from pathlib import Path

import dagster as dg
from dagster.components import definitions
from dagster_dbt import DbtCliResource, DbtProject


@cache
def execucao_orcamentaria_dbt_project() -> DbtProject:
    project = DbtProject(
        project_dir=str((Path.cwd() / "execucao_orcamentaria_dbt").resolve()),
        target="dev",
    )
    project.prepare_if_dev()
    return project


@definitions
def defs():
    return dg.Definitions(
        resources={
            "dbt": DbtCliResource(
                project_dir=execucao_orcamentaria_dbt_project()
            )
        }
    )
