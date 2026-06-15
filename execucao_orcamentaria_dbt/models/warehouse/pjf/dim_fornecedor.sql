select
    sk_fornecedor,
    cd_cpf_cnpj,
    nm_fornecedor
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_fornecedor order by dt_atualizacao desc) = 1
