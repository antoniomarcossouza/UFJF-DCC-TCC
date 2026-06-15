{{ config(materialized='ephemeral') }}

with base as (
    select
        nm_arquivo,
        dt_atualizacao,
        2000 + cast(left(split_part(lower(nm_arquivo), '.', 1), 2) as int)
            as nu_ano_referencia,
        cast(right(split_part(lower(nm_arquivo), '.', 1), 2) as int)
            as nu_mes_referencia,
        "Unidade Administrativa" as nm_unidade_administrativa,
        "Nº da Nota de Empenho" as nu_nota_empenho,
        "Modalidade
Empenho" as ds_modalidade_empenho,
        licitação as ds_licitacao,
        "Referência
Legal" as ds_referencia_legal,
        processo as cd_processo,
        descrição as ds_empenho,
        try_cast("Data do Empenho" as date) as dt_empenho,
        try_cast("Data da Liquidação" as date) as dt_liquidacao,
        "CPF/CNPJ" as cd_cpf_cnpj,
        fornecedor as nm_fornecedor,
        "Funcional Programática" as cd_funcional_pragmatica,
        "Descrição da Ação" as ds_funcional_pragmatica,
        "Natureza
de Despesa" as cd_natureza_despesa,
        "Descrição Natureza de Despesa" as ds_natureza_despesa,
        fonte as cd_fonte_recurso,
        "Descrição da Fonte" as ds_fonte_recurso,
        cast("Vr.Empenhado
No Mês" as numeric(18, 2)) as vl_empenhado_mes,
        cast("Vr. Liquidado
No Mês" as numeric(18, 2)) as vl_liquidado_mes,
        cast("Vr. Pago
No Mês" as numeric(18, 2)) as vl_pago_mes
    from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'nm_unidade_administrativa',
            'nu_nota_empenho',
            'cd_processo',
        ]) }} as sk_empenho,
        {{ dbt_utils.generate_surrogate_key(['nm_unidade_administrativa']) }}
            as sk_unidade_administrativa,
        case
            when dt_empenho is not null
                then {{ sk_tempo_dia_data_expr('dt_empenho') }}
        end as sk_tempo_empenho,
        case
            when dt_liquidacao is not null
                then {{ sk_tempo_dia_data_expr('dt_liquidacao') }}
        end as sk_tempo_liquidacao,
        {{ dbt_utils.generate_surrogate_key(['cd_cpf_cnpj']) }} as sk_fornecedor,
        {{ dbt_utils.generate_surrogate_key(['cd_funcional_pragmatica']) }}
            as sk_funcional_pragmatica,
        {{ dbt_utils.generate_surrogate_key(['cd_natureza_despesa']) }}
            as sk_natureza_despesa,
        {{ dbt_utils.generate_surrogate_key(['cd_fonte_recurso']) }}
            as sk_fonte_recurso,
        nm_arquivo,
        dt_atualizacao,
        nu_ano_referencia,
        nu_mes_referencia,
        nm_unidade_administrativa,
        nu_nota_empenho,
        ds_modalidade_empenho,
        ds_licitacao,
        ds_referencia_legal,
        cd_processo,
        ds_empenho,
        dt_empenho,
        dt_liquidacao,
        cd_cpf_cnpj,
        nm_fornecedor,
        cd_funcional_pragmatica,
        ds_funcional_pragmatica,
        cd_natureza_despesa,
        ds_natureza_despesa,
        cd_fonte_recurso,
        ds_fonte_recurso,
        vl_empenhado_mes,
        vl_liquidado_mes,
        vl_pago_mes
    from base
)

select k.*
from keyed as k
left join {{ ref('dim_tempo') }} as t_emp
    on k.sk_tempo_empenho = t_emp.sk_tempo
left join {{ ref('dim_tempo') }} as t_liq
    on k.sk_tempo_liquidacao = t_liq.sk_tempo
