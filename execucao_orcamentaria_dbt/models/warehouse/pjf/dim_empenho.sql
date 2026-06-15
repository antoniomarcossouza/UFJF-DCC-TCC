select
    sk_empenho,
    nu_nota_empenho,
    ds_modalidade_empenho,
    ds_licitacao,
    ds_referencia_legal,
    cd_processo,
    ds_empenho
from {{ ref('int_pjf_despesa_mensal_consolidada') }}
qualify row_number() over (partition by sk_empenho order by dt_atualizacao desc) = 1
