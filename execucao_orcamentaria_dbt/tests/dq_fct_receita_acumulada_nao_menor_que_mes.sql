with invalid_rows as (
    select
        m.sk_natureza_receita,
        m.nu_ano_referencia,
        m.nu_mes_referencia,
        m.vl_arrecadada_mes,
        a.vl_arrecadada_ano
    from {{ ref('fct_receita') }} as m
    inner join {{ ref('fct_receita_acumulada') }} as a
        on m.sk_natureza_receita = a.sk_natureza_receita
        and m.nu_ano_referencia = a.nu_ano_referencia
        and m.nu_mes_referencia = a.nu_mes_referencia
    where coalesce(a.vl_arrecadada_ano, 0) < coalesce(m.vl_arrecadada_mes, 0)
)
select *
from invalid_rows
