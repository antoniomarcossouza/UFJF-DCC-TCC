select
    sk_funcional_pragmatica,
    cd_funcional_pragmatica,
    ds_funcional_pragmatica
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_funcional_pragmatica order by dt_atualizacao desc) = 1
