with source as (select * from {{ source('staging', 'pjf_receita_mensal_prevista') }})

select *
from source
qualify rank() over (partition by nm_arquivo order by dt_atualizacao desc) = 1
