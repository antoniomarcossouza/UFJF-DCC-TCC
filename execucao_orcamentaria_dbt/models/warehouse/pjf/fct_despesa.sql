select
    {{ dbt_utils.generate_surrogate_key([
        '"Nº da Nota de Empenho"',
        '"Processo"',
        '"Descrição"'
    ]) }} as sk_empenho,
    {{ dbt_utils.generate_surrogate_key(['"Unidade Administrativa"']) }}
        as sk_unidade_administrativa,
    "Data do Empenho" as dt_empenho, -- SK_TEMPO_EMPENHO
    "Data da Liquidação" as dt_liquidacao, -- SK_TEMPO_LIQUIDACAO
    {{ dbt_utils.generate_surrogate_key(['"CPF/CNPJ"']) }} as sk_fornecedor,
    {{ dbt_utils.generate_surrogate_key(['"Funcional Programática"']) }} as sk_funcional_pragmatica,
    {{ dbt_utils.generate_surrogate_key(['"Natureza
de Despesa"']) }} as sk_natureza_despesa,
    {{ dbt_utils.generate_surrogate_key(['"Fonte"']) }} as sk_fonte_recurso,
    "Vr.Empenhado
No Mês" as vl_empenhado_mes,
    "Vr. Liquidado
No Mês" as vl_liquidado_mes,
    "Vr. Pago
No Mês" as vl_pago_mes
from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
