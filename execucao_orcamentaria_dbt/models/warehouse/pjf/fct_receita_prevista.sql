select
    "Natureza De Receita",
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
    "Total"
from {{ ref('stg_pjf_receita_mensal_prevista') }}