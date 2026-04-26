{{ config(materialized='ephemeral') }}

with base as (
    select
        cast("Arrecadada Mês" as numeric(18,2)) as vl_arrecadada_mes,
        regexp_replace(replace("Natureza", '.0', ''), '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(left(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_mes_referencia,
        cast("Previsão Inicial" as numeric(18,2)) as vl_previsao_inicial_comparativa,
        cast("Previsão Atualizada" as numeric(18,2)) as vl_previsao_atualizada,
        cast("Arrecadada Ano" as numeric(18,2)) as vl_arrecadada_ano,
        cast("A Realizar" as numeric(18,2)) as vl_a_realizar
    from {{ ref('stg_pjf_receita_mensal_comparativa') }}
    where "Natureza" <> 'TOTAIS GERAIS'
)
select
    {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
    vl_arrecadada_mes,
    cd_natureza_receita,
    nu_ano_referencia,
    nu_mes_referencia,
    vl_previsao_inicial_comparativa,
    vl_previsao_atualizada,
    vl_arrecadada_ano,
    vl_a_realizar
from base
