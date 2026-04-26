with prevista_mensal as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
        cd_natureza_receita,
        nu_mes_referencia,
        vl_previsto_mensal
    from (
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 1 as nu_mes_referencia, cast("Janeiro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 2 as nu_mes_referencia, cast("Fevereiro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 3 as nu_mes_referencia, cast("Março" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 4 as nu_mes_referencia, cast("Abril" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 5 as nu_mes_referencia, cast("Maio" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 6 as nu_mes_referencia, cast("Junho" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 7 as nu_mes_referencia, cast("Julho" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 8 as nu_mes_referencia, cast("Agosto" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 9 as nu_mes_referencia, cast("Setembro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 10 as nu_mes_referencia, cast("Outubro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 11 as nu_mes_referencia, cast("Novembro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
        union all
        select regexp_replace("Natureza De Receita", '[^0-9]', '', 'g') as cd_natureza_receita, 12 as nu_mes_referencia, cast("Dezembro" as numeric(18,2)) as vl_previsto_mensal from {{ ref('stg_pjf_receita_mensal_prevista') }}
    ) src
),
comparativa as (
    select
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_receita']) }} as sk_natureza_receita,
        vl_previsao_inicial_comparativa,
        vl_previsao_atualizada,
        vl_arrecadada_mes,
        vl_arrecadada_ano,
        vl_a_realizar,
        cd_natureza_receita,
        nu_mes_referencia
    from (
        select
            "Previsão Inicial" as vl_previsao_inicial_comparativa,
            "Previsão Atualizada" as vl_previsao_atualizada,
            "Arrecadada Mês" as vl_arrecadada_mes,
            "Arrecadada Ano" as vl_arrecadada_ano,
            "A Realizar" as vl_a_realizar,
            regexp_replace(replace("Natureza", '.0', ''), '[^0-9]', '', 'g') as cd_natureza_receita,
            cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int) as nu_mes_referencia
        from {{ ref('stg_pjf_receita_mensal_comparativa') }}
        where "Natureza" <> 'TOTAIS GERAIS'
    ) src
)
select
    coalesce(c.sk_natureza_receita, p.sk_natureza_receita) as sk_natureza_receita,
    coalesce(c.cd_natureza_receita, p.cd_natureza_receita) as cd_natureza_receita,
    coalesce(c.nu_mes_referencia, p.nu_mes_referencia) as nu_mes_referencia,
    c.vl_previsao_inicial_comparativa,
    p.vl_previsto_mensal,
    c.vl_previsao_atualizada,
    c.vl_arrecadada_mes,
    c.vl_arrecadada_ano,
    c.vl_a_realizar,
    coalesce(c.vl_previsao_inicial_comparativa, 0) - coalesce(p.vl_previsto_mensal, 0) as vl_diferenca_previsao,
    coalesce(c.vl_previsao_inicial_comparativa, 0) <> coalesce(p.vl_previsto_mensal, 0) as fl_tem_discrepancia_previsao
from comparativa c
full outer join prevista_mensal p
    on c.sk_natureza_receita = p.sk_natureza_receita
    and c.nu_mes_referencia = p.nu_mes_referencia