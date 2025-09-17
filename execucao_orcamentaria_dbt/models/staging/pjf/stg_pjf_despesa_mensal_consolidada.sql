with source as (select * from {{ source('staging', 'pjf_despesa_mensal_consolidada') }})

select *
from source
qualify rank() over (partition by nm_arquivo order by dt_atualizacao desc) = 1
