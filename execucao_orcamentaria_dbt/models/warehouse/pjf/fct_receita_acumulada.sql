select
    sk_natureza_receita,
    sk_tempo_referencia,
    vl_previsao_inicial_comparativa,
    vl_previsao_atualizada,
    vl_arrecadada_ano,
    vl_a_realizar
from {{ ref('int_pjf_receita_comparativa') }}
