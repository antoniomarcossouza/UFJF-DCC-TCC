select
    "Previsão Inicial",
    "Previsão Atualizada",
    "Arrecadada Mês",
    "Arrecadada Ano",
    "A Realizar",
    "Natureza",
    "Fonte TCE"
from {{ ref('stg_pjf_receita_mensal_comparativa') }}