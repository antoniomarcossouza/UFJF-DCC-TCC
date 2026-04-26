with
prevista_mensal as (
    select
        sk_natureza_receita,
        sk_tempo_referencia,
        vl_previsto_mensal
    from {{ ref('int_pjf_receita_prevista_mensal') }}
),

comparativa as (
    select
        sk_natureza_receita,
        sk_tempo_referencia,
        vl_arrecadada_mes
    from {{ ref('int_pjf_receita_comparativa') }}
)

select
    coalesce(c.sk_natureza_receita, p.sk_natureza_receita) as sk_natureza_receita,
    coalesce(c.sk_tempo_referencia, p.sk_tempo_referencia) as sk_tempo_referencia,
    coalesce(p.vl_previsto_mensal, 0) as vl_previsto_mensal,
    coalesce(c.vl_arrecadada_mes, 0) as vl_arrecadada_mes
from comparativa as c
full outer join prevista_mensal as p
    on
        c.sk_natureza_receita = p.sk_natureza_receita
        and c.sk_tempo_referencia = p.sk_tempo_referencia
