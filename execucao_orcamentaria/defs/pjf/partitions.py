from datetime import datetime

import dagster as dg

year_month_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 * *",
    fmt="%y%m",
)

year_partition = dg.TimeWindowPartitionsDefinition(
    start=datetime(2020, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%y",
    end_offset=1,
)
