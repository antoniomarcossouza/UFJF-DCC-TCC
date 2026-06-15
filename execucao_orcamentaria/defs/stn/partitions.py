from datetime import datetime

import dagster as dg

year_partition_stn = dg.TimeWindowPartitionsDefinition(
    start=datetime(2021, 1, 1),
    cron_schedule="0 0 1 1 *",
    fmt="%Y",
    end_offset=1,
)
