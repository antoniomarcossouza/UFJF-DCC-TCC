select
	{{ dbt_utils.generate_surrogate_key(['"Unidade Administrativa"']) }} as sk_unidade_administrativa,
	"Data do Empenho" as dt_empenho, -- SK_TEMPO_EMPENHO
	"Data da Liquidação" as dt_liquidacao, -- SK_TEMPO_LIQUIDACAO
    {{ dbt_utils.generate_surrogate_key(['"CPF/CNPJ"']) }} as sk_fornecedor,
	"Funcional Programática" as nu_funcional_progragmatica, -- DIM_CLASSIFICACAO_ORCAMENTARIA
	"Descrição da Ação", -- DIM_CLASSIFICACAO_ORCAMENTARIA
	"Natureza
de Despesa" as cd_natureza_despesa, -- DIM_CLASSIFICACAO_ORCAMENTARIA
	"Descrição Natureza de Despesa" as ds_natureza_despesa, -- DIM_CLASSIFICACAO_ORCAMENTARIA
	"Fonte" as cd_fonte, -- DIM_CLASSIFICACAO_ORCAMENTARIA
	"Descrição da Fonte" as ds_fonte, -- DIM_CLASSIFICACAO_ORCAMENTARIA
	"Vr.Empenhado
No Mês" as vl_empenhado_mes,
	"Vr. Liquidado
No Mês" as vl_liquidado_mes,
	"Vr. Pago
No Mês" as vl_pago_mes
from execucao_orcamentaria.stg.stg_pjf_despesa_mensal_consolidada;