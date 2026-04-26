with stn_latest as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }}
            as sk_natureza_receita,
        cd_natureza_receita,
        ds_natureza_receita,
        nu_ano_referencia,
        {{ sk_tempo_dia_data_expr("make_date(nu_ano_referencia, 1, 1)") }}
            as sk_tempo_ementa
    from {{ ref('stg_stn_ementario_natureza_receita') }}
    qualify row_number() over (
        partition by cd_natureza_receita
        order by nu_ano_referencia desc, length(ds_natureza_receita) desc
    ) = 1
),

pjf_union as (
    select
        cd_natureza_receita,
        sk_tempo_referencia
    from {{ ref('int_pjf_receita_prevista_mensal') }}
    union all
    select
        cd_natureza_receita,
        sk_tempo_referencia
    from {{ ref('int_pjf_receita_comparativa') }}
),

pjf_references as (
    select
        u.cd_natureza_receita,
        min(t.nu_ano) as nu_ano_referencia
    from pjf_union as u
    inner join {{ ref('dim_tempo') }} as t
        on u.sk_tempo_referencia = t.sk_tempo
    where u.cd_natureza_receita is not null and u.cd_natureza_receita <> ''
    group by u.cd_natureza_receita
),

pjf_only as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.cd_natureza_receita']) }}
            as sk_natureza_receita,
        p.cd_natureza_receita,
        'Sem ementa STN (origem PJF)' as ds_natureza_receita,
        p.nu_ano_referencia,
        {{ sk_tempo_dia_data_expr("make_date(p.nu_ano_referencia, 1, 1)") }}
            as sk_tempo_ementa
    from pjf_references as p
    left join stn_latest as s
        on p.cd_natureza_receita = s.cd_natureza_receita
    where s.cd_natureza_receita is null
),

combined as (
    select * from stn_latest
    union all
    select * from pjf_only
)

select
    c.sk_natureza_receita,
    c.cd_natureza_receita,
    c.ds_natureza_receita,
    c.sk_tempo_ementa
from combined as c
inner join {{ ref('dim_tempo') }} as t
    on c.sk_tempo_ementa = t.sk_tempo
