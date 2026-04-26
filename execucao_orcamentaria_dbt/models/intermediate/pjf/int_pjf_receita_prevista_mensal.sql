{{ config(materialized='ephemeral') }}

with base as (
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        1 as nu_mes_referencia,
        coalesce(cast(janeiro as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        2 as nu_mes_referencia,
        coalesce(cast(fevereiro as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        3 as nu_mes_referencia,
        coalesce(cast("Março" as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        4 as nu_mes_referencia,
        coalesce(cast(abril as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        5 as nu_mes_referencia,
        coalesce(cast(maio as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        6 as nu_mes_referencia,
        coalesce(cast(junho as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        7 as nu_mes_referencia,
        coalesce(cast(julho as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        8 as nu_mes_referencia,
        coalesce(cast(agosto as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        9 as nu_mes_referencia,
        coalesce(cast(setembro as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        10 as nu_mes_referencia,
        coalesce(cast(outubro as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        11 as nu_mes_referencia,
        coalesce(cast(novembro as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select
        regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        12 as nu_mes_referencia,
        coalesce(cast(dezembro as numeric(18, 2)), 0) as vl_previsto_mensal
    from {{ ref('stg_pjf_receita_mensal_prevista') }}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
        cd_natureza_receita,
        {{
            sk_tempo_dia_data_expr(
                "make_date(nu_ano_referencia, nu_mes_referencia, 1)"
            )
        }} as sk_tempo_referencia,
        vl_previsto_mensal
    from base
    where cd_natureza_receita is not null
)

select
    k.sk_natureza_receita,
    k.cd_natureza_receita,
    k.sk_tempo_referencia,
    k.vl_previsto_mensal
from keyed as k
inner join {{ ref('dim_tempo') }} as t
    on k.sk_tempo_referencia = t.sk_tempo
