with prevista_mensal as (
    select *
    from {{ ref('int_pjf_receita_prevista_mensal') }}
),
comparativa as (
    select
        sk_natureza_receita,
        vl_arrecadada_mes,
        cd_natureza_receita,
        nu_ano_referencia,
        nu_mes_referencia
    from {{ ref('int_pjf_receita_comparativa') }}
)
select
    c.sk_natureza_receita as sk_natureza_receita,
    c.nu_ano_referencia as nu_ano_referencia,
    c.nu_mes_referencia as nu_mes_referencia,
    p.vl_previsto_mensal,
    c.vl_arrecadada_mes
from comparativa c
full outer join prevista_mensal p
    on c.sk_natureza_receita = p.sk_natureza_receita
    and c.nu_ano_referencia = p.nu_ano_referencia
    and c.nu_mes_referencia = p.nu_mes_referencia