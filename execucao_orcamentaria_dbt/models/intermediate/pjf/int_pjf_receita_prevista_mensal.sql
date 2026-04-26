{{ config(materialized='ephemeral') }}

with base as (
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 1 as nu_mes_referencia, cast("Janeiro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 2 as nu_mes_referencia, cast("Fevereiro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 3 as nu_mes_referencia, cast("Março" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 4 as nu_mes_referencia, cast("Abril" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 5 as nu_mes_referencia, cast("Maio" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 6 as nu_mes_referencia, cast("Junho" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 7 as nu_mes_referencia, cast("Julho" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 8 as nu_mes_referencia, cast("Agosto" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 9 as nu_mes_referencia, cast("Setembro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 10 as nu_mes_referencia, cast("Outubro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 11 as nu_mes_referencia, cast("Novembro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    union all
    select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2000 + cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia, 12 as nu_mes_referencia, cast("Dezembro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
    cd_natureza_receita,
    nu_ano_referencia,
    nu_mes_referencia,
    vl_previsto_mensal
from base
