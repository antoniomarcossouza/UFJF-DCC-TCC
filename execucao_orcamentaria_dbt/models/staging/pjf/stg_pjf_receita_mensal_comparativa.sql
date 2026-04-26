with source as (
    select * from {{ source('staging', 'pjf_receita_mensal_comparativa') }}
    where natureza <> 'TOTAIS GERAIS'

),

dedup as (
    select *
    from source
    qualify rank() over (partition by nm_arquivo order by dt_atualizacao desc) = 1
)

select
    natureza,
    sum("Previsão Inicial") as vl_previsao_inicial,
    sum("Previsão Atualizada") as vl_previsao_atualizada,
    sum("Arrecadada Mês") as vl_arrecadada_mes,
    sum("Arrecadada Ano") as vl_arrecadada_ano,
    sum("A Realizar") as vl_a_realizar,
    nm_arquivo,
    dt_atualizacao
from dedup
group by natureza, nm_arquivo, dt_atualizacao
