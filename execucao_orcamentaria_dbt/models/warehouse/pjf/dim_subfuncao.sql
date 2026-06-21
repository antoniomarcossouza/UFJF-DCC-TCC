with ranked as (
    select
        *,
        row_number() over (
            partition by cd_subfuncao
            order by nu_ano_referencia desc, length(ds_subfuncao) desc
        ) as rn
    from {{ ref('stg_pjf_loa_subfuncao') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cd_subfuncao']) }} as sk_subfuncao,
    cd_subfuncao,
    ds_subfuncao,
    {{ sk_tempo_dia_data_expr("make_date(nu_ano_referencia, 1, 1)") }}
        as sk_tempo_ementa
from ranked
where rn = 1
