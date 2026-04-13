select distinct
    {{ dbt_utils.generate_surrogate_key(['"Fonte"']) }} as sk_fonte_recurso,
	"Fonte" as cd_fonte_recurso,
	"Descrição da Fonte" as ds_fonte_recurso
from {{ref('stg_pjf_despesa_mensal_consolidada')}}
