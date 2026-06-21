with pjf_base as (
    select
        sk_natureza_despesa,
        cd_natureza_despesa,
        ds_natureza_despesa,
        nu_ano_referencia,
        nu_mes_referencia
    from {{ ref('int_pjf_despesa_mensal_consolidada') }}
    qualify row_number() over (
        partition by sk_natureza_despesa
        order by dt_atualizacao desc
    ) = 1
),

pjf_sem_codigo as (
    select
        sk_natureza_despesa,
        coalesce(nullif(trim(cd_natureza_despesa), ''), '—') as cd_natureza_despesa,
        coalesce(
            nullif(trim(ds_natureza_despesa), ''),
            'Natureza não informada'
        ) as ds_natureza_despesa,
        {{ sk_tempo_dia_data_expr("make_date(coalesce(nu_ano_referencia, 2000), 1, 1)") }}
            as sk_tempo_ementa
    from pjf_base
    where
        cd_natureza_despesa is null
        or trim(cd_natureza_despesa) = ''
),

pjf_com_codigo as (
    select
        sk_natureza_despesa,
        cd_natureza_despesa,
        coalesce(
            nullif(trim(ds_natureza_despesa), ''),
            trim(cd_natureza_despesa)
        ) as ds_natureza_despesa,
        {{ sk_tempo_dia_data_expr("make_date(nu_ano_referencia, 1, 1)") }}
            as sk_tempo_ementa
    from pjf_base
    where
        cd_natureza_despesa is not null
        and trim(cd_natureza_despesa) <> ''
),

combined as (
    select * from pjf_com_codigo
    union all
    select * from pjf_sem_codigo
)

select
    c.sk_natureza_despesa,
    c.cd_natureza_despesa,
    c.ds_natureza_despesa,
    c.sk_tempo_ementa
from combined as c
inner join {{ ref('dim_tempo') }} as t
    on c.sk_tempo_ementa = t.sk_tempo
