with ranked as (
    select
        *,
        row_number() over (
            partition by cd_funcao
            order by nu_ano_referencia desc, length(ds_funcao) desc
        ) as rn
    from {{ ref('stg_pjf_loa_funcao') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cd_funcao']) }} as sk_funcao,
    cd_funcao,
    ds_funcao,
    {{ sk_tempo_dia_data_expr("make_date(nu_ano_referencia, 1, 1)") }}
        as sk_tempo_ementa
from ranked
where rn = 1
