select distinct
    {{ dbt_utils.generate_surrogate_key(['"CPF/CNPJ"']) }} as sk_fornecedor,
    "CPF/CNPJ" as cd_cpf_cnpj,
    fornecedor as nm_fornecedor
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_fornecedor order by dt_atualizacao desc) = 1
