select distinct
    {{ dbt_utils.generate_surrogate_key(['"Natureza
de Despesa"']) }} as sk_natureza_despesa,
	"Natureza
de Despesa" as cd_natureza_despesa,
	"Descrição Natureza de Despesa" as ds_natureza_despesa
from {{ref('stg_pjf_despesa_mensal_consolidada')}}
