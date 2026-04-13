select
	"Nº da Nota de Empenho" as nu_nota_empenho,
	"Modalidade
Empenho" as ds_modalidade_empenho, 
	"Licitação" as ds_licitacao,
	"Referência
Legal" as ds_referencia_legal,
	"Processo" as cd_processo,
	"Descrição" as ds_empenho
from {{ref('stg_pjf_despesa_mensal_consolidada')}}


/*
Separar em fato_empenho_item e dim_empenho? Não sei.
*/