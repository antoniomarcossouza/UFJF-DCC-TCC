select distinct
    {{ dbt_utils.generate_surrogate_key(['"Unidade Administrativa"']) }}
        as sk_unidade_administrativa,
    "Unidade Administrativa" as nm_unidade_administrativa
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_unidade_administrativa order by dt_atualizacao desc) = 1