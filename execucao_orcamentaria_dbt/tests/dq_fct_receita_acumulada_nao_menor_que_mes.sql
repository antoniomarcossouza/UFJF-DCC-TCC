with invalid_rows as (
    select
        m.sk_natureza_receita,
        m.sk_tempo_referencia,
        m.vl_arrecadada_mes,
        a.vl_arrecadada_ano
    from {{ ref('fct_receita') }} as m
    inner join {{ ref('fct_receita_acumulada') }} as a
        on
            m.sk_natureza_receita = a.sk_natureza_receita
            and m.sk_tempo_referencia = a.sk_tempo_referencia
    where coalesce(a.vl_arrecadada_ano, 0) < coalesce(m.vl_arrecadada_mes, 0)
)

select *
from invalid_rows
