with invalid_rows as (
    select
        sk_natureza_receita,
        sk_tempo_referencia,
        vl_previsao_atualizada,
        vl_arrecadada_ano,
        vl_a_realizar
    from {{ ref('fct_receita_acumulada') }}
    where
        coalesce(vl_a_realizar, 0)
        <> coalesce(vl_previsao_atualizada, 0) - coalesce(vl_arrecadada_ano, 0)
)

select *
from invalid_rows
