select distinct
    {{ dbt_utils.generate_surrogate_key(['"CPF/CNPJ"']) }} as sk_fornecedor,
	"CPF/CNPJ" as cd_cpf_cnpj,
    "FORNECEDOR" as nm_fornecedor,
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
