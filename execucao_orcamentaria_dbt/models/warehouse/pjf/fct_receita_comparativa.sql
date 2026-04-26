select
    "Previsão Inicial" as vl_previsao_inicial,
    "Previsão Atualizada" as vl_previsao_atualizada,
    "Arrecadada Mês" as vl_arrecadada_mes,
    "Arrecadada Ano" as vl_arrecadada_ano,
    "A Realizar" as vl_a_realizar,
    "Natureza" as nm_natureza_receita,
    nm_arquivo as nm_arquivo_origem,
    "Fonte TCE" as nm_fonte_tce,
    right(split_part(lower(nm_arquivo), '.', 1), 2) as nu_mes_referencia
from {{ ref('stg_pjf_receita_mensal_comparativa') }}