select distinct
        {{ dbt_utils.generate_surrogate_key([
            '"Nº da Nota de Empenho"',
            '"Processo"',
            '"Descrição"'
        ]) }} as sk_empenho,
        "Nº da Nota de Empenho" as nu_nota_empenho,
        "Modalidade
Empenho" as ds_modalidade_empenho,
        "Licitação" as ds_licitacao,
        "Referência
Legal" as ds_referencia_legal,
        "Processo" as cd_processo,
        "Descrição" as ds_empenho
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
