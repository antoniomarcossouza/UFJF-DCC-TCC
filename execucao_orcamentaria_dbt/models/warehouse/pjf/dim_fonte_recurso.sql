select distinct
    {{ dbt_utils.generate_surrogate_key(['"Fonte"']) }} as sk_fonte_recurso,
    fonte as cd_fonte_recurso,
    "Descrição da Fonte" as ds_fonte_recurso
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_fonte_recurso order by dt_atualizacao desc) = 1
