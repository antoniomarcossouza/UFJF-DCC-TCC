with base as (
    select
        sk_natureza_receita,
        nu_ano_referencia,
        nu_mes_referencia,
        vl_arrecadada_ano,
        lag(vl_arrecadada_ano) over (
            partition by sk_natureza_receita, nu_ano_referencia
            order by nu_mes_referencia
        ) as vl_arrecadada_ano_mes_anterior
    from {{ ref('fct_receita_acumulada') }}
),
invalid_rows as (
    select
        sk_natureza_receita,
        nu_ano_referencia,
        nu_mes_referencia,
        vl_arrecadada_ano_mes_anterior,
        vl_arrecadada_ano
    from base
    where vl_arrecadada_ano_mes_anterior is not null
      and coalesce(vl_arrecadada_ano, 0) < coalesce(vl_arrecadada_ano_mes_anterior, 0)
)
select *
from invalid_rows
