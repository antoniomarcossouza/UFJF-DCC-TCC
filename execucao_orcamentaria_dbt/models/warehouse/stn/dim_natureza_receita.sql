select
    {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
    cd_natureza_receita,
    ds_natureza_receita,
    nu_ano_referencia
from {{ ref('stg_stn_ementario_natureza_receita') }}
qualify row_number() over (
    partition by cd_natureza_receita
    order by nu_ano_referencia desc, length(ds_natureza_receita) desc
) = 1
