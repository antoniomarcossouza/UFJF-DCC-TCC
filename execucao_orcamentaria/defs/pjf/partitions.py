from datetime import datetime

import dagster as dg

year_month_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2021, 1, 1),
    cron_schedule="0 0 1 * *",
    fmt="%y%m",
)

year_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2021, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%y",
    end_offset=1,
)

# LOA DimLOA: layout do PDF varia por exercício; parser validado só para 2026.
year_partition_loa = dg.TimeWindowPartitionsDefinition(
    start=datetime(2026, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%Y",
    end_offset=0,
)
