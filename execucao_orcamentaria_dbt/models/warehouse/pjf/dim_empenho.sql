select distinct
        {{ dbt_utils.generate_surrogate_key([
            '"Nº da Nota de Empenho"',
            '"Processo"',
            '"Descrição"'
        ]) }} as sk_empenho,
    "Nº da Nota de Empenho" as nu_nota_empenho,
    "Modalidade
Empenho" as ds_modalidade_empenho,
    licitação as ds_licitacao,
    "Referência
Legal" as ds_referencia_legal,
    processo as cd_processo,
    descrição as ds_empenho
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_empenho order by dt_atualizacao desc) = 1