with source as (
    select * from {{ source('staging', 'pjf_receita_mensal_comparativa') }}
    where "Natureza" <> 'TOTAIS GERAIS'
    
),

dedup as (
    select *
    from source
    qualify rank() over (partition by nm_arquivo order by dt_atualizacao desc) = 1
)

select
    "Natureza", 
    sum("Previsão Inicial") as "Previsão Inicial", 
    sum("Previsão Atualizada") as "Previsão Atualizada", 
    sum("Arrecadada Mês") as "Arrecadada Mês", 
    sum("Arrecadada Ano") as "Arrecadada Ano", 
    sum("A Realizar") as "A Realizar", 
    nm_arquivo, 
    dt_atualizacao
from dedup
group by "Natureza", nm_arquivo, dt_atualizacao
