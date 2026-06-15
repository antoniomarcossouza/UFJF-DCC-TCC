select
    sk_empenho,
    sk_unidade_administrativa,
    sk_tempo_empenho,
    sk_tempo_liquidacao,
    sk_fornecedor,
    sk_funcional_pragmatica,
    sk_natureza_despesa,
    sk_fonte_recurso,
    vl_empenhado_mes,
    vl_liquidado_mes,
    vl_pago_mes
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
