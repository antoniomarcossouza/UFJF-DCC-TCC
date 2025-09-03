import dagster as dg

import execucao_orcamentaria.defs


@dg.components.definitions
def defs() -> dg.Definitions:
    return dg.components.load_defs(execucao_orcamentaria.defs)
