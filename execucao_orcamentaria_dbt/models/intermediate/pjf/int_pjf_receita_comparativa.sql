{{ config(materialized='ephemeral') }}

with base as (
    select
        cast(vl_arrecadada_mes as numeric(18, 2)) as vl_arrecadada_mes,
        regexp_replace(replace(natureza, '.0', ''), '[^0-9]', '', 'g') as cd_natureza_receita,
        2000 + cast(left(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_ano_referencia,
        cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_mes_referencia,
        cast(vl_previsao_inicial as numeric(18, 2)) as vl_previsao_inicial_comparativa,
        cast(vl_previsao_atualizada as numeric(18, 2)) as vl_previsao_atualizada,
        cast(vl_arrecadada_ano as numeric(18, 2)) as vl_arrecadada_ano,
        cast(vl_a_realizar as numeric(18, 2)) as vl_a_realizar
    from {{ ref('stg_pjf_receita_mensal_comparativa') }}
    where natureza <> 'TOTAIS GERAIS'
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
        vl_arrecadada_mes,
        cd_natureza_receita,
        {{
            sk_tempo_dia_data_expr(
                "make_date(nu_ano_referencia, nu_mes_referencia, 1)"
            )
        }} as sk_tempo_referencia,
        vl_previsao_inicial_comparativa,
        vl_previsao_atualizada,
        vl_arrecadada_ano,
        vl_a_realizar
    from base
)

select
    k.sk_natureza_receita,
    k.vl_arrecadada_mes,
    k.cd_natureza_receita,
    k.sk_tempo_referencia,
    k.vl_previsao_inicial_comparativa,
    k.vl_previsao_atualizada,
    k.vl_arrecadada_ano,
    k.vl_a_realizar
from keyed as k
inner join {{ ref('dim_tempo') }} as t
    on k.sk_tempo_referencia = t.sk_tempo
