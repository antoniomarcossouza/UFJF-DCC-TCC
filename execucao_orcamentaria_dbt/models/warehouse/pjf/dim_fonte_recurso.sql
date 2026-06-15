select
    sk_fonte_recurso,
    cd_fonte_recurso,
    ds_fonte_recurso
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_fonte_recurso order by dt_atualizacao desc) = 1
