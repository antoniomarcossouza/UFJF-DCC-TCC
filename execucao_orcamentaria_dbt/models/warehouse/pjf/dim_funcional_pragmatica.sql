select distinct
    {{ dbt_utils.generate_surrogate_key(['"Funcional Programática"']) }} as sk_funcional_pragmatica,
    "Funcional Programática" as cd_funcional_pragmatica,
    "Descrição da Ação" as ds_funcional_pragmatica
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_funcional_pragmatica order by dt_atualizacao desc) = 1
