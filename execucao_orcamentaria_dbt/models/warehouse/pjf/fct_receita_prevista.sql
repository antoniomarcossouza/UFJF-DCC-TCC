select
    "Natureza De Receita" as nm_natureza_receita,
    "Janeiro" as vl_previsto_janeiro,
    "Fevereiro" as vl_previsto_fevereiro,
    "Março" as vl_previsto_marco,
    "Abril" as vl_previsto_abril,
    "Maio" as vl_previsto_maio,
    "Junho" as vl_previsto_junho,
    "Julho" as vl_previsto_julho,
    "Agosto" as vl_previsto_agosto,
    "Setembro" as vl_previsto_setembro,
    "Outubro" as vl_previsto_outubro,
    "Novembro" as vl_previsto_novembro,
    "Dezembro" as vl_previsto_dezembro,
    "Total" as vl_previsto_total
from {{ ref('stg_pjf_receita_mensal_prevista') }}