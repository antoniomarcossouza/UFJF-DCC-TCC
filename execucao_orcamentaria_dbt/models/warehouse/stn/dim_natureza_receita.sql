with
base as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
        cd_natureza_receita,
        ds_natureza_receita,
        nu_ano_referencia,
        {{ dbt_utils.generate_surrogate_key(
            ["strftime(make_date(nu_ano_referencia, 1, 1), '%Y-%m-%d')"]
        ) }} as sk_tempo_ementa
    from {{ ref('stg_stn_ementario_natureza_receita') }}
),

joined as (
    select
        b.sk_natureza_receita,
        b.cd_natureza_receita,
        b.ds_natureza_receita,
        b.nu_ano_referencia,
        b.sk_tempo_ementa
    from base as b
    inner join {{ ref('dim_tempo') }} as t
        on b.sk_tempo_ementa = t.sk_tempo
)

select
    sk_natureza_receita,
    cd_natureza_receita,
    ds_natureza_receita,
    sk_tempo_ementa
from joined
qualify row_number() over (
    partition by cd_natureza_receita
    order by nu_ano_referencia desc, length(ds_natureza_receita) desc
) = 1
