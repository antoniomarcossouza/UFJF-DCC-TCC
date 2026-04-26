with base as (
    select
        a.sk_natureza_receita,
        t.nu_ano,
        t.nu_mes,
        a.vl_arrecadada_ano,
        lag(a.vl_arrecadada_ano) over (
            partition by a.sk_natureza_receita, t.nu_ano
            order by t.nu_mes
        ) as vl_arrecadada_ano_mes_anterior
    from {{ ref('fct_receita_acumulada') }} as a
    inner join {{ ref('dim_tempo') }} as t
        on a.sk_tempo_referencia = t.sk_tempo
),

invalid_rows as (
    select
        sk_natureza_receita,
        nu_ano,
        nu_mes,
        vl_arrecadada_ano_mes_anterior,
        vl_arrecadada_ano
    from base
    where
        vl_arrecadada_ano_mes_anterior is not null
        and coalesce(vl_arrecadada_ano, 0) < coalesce(vl_arrecadada_ano_mes_anterior, 0)
)

select *
from invalid_rows
