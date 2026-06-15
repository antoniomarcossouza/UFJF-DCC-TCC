select
    sk_natureza_despesa,
    cd_natureza_despesa,
    ds_natureza_despesa
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_natureza_despesa order by dt_atualizacao desc) = 1
