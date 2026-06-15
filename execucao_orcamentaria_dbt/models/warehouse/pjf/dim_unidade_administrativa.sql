select
    sk_unidade_administrativa,
    nm_unidade_administrativa
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_unidade_administrativa order by dt_atualizacao desc) = 1
