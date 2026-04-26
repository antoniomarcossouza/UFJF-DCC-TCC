with
marked as (
    select
        {{ dbt_utils.generate_surrogate_key([
            '"Nº da Nota de Empenho"',
            '"Processo"',
            '"Descrição"'
        ]) }} as sk_empenho,
        {{ dbt_utils.generate_surrogate_key(['"Unidade Administrativa"']) }}
            as sk_unidade_administrativa,
        case
            when try_cast("Data do Empenho" as date) is not null
                then
                    {{ sk_tempo_dia_data_expr('try_cast("Data do Empenho" as date)') }}
        end as sk_tempo_empenho,
        case
            when try_cast("Data da Liquidação" as date) is not null
                then
                    {{ sk_tempo_dia_data_expr('try_cast("Data da Liquidação" as date)') }}
        end as sk_tempo_liquidacao,
        {{ dbt_utils.generate_surrogate_key(['"CPF/CNPJ"']) }} as sk_fornecedor,

        {{ dbt_utils.generate_surrogate_key(['"Funcional Programática"']) }}
            as sk_funcional_pragmatica,
        {{ dbt_utils.generate_surrogate_key(['"Natureza
de Despesa"']) }} as sk_natureza_despesa,
        {{ dbt_utils.generate_surrogate_key(['"Fonte"']) }} as sk_fonte_recurso,
        cast("Vr.Empenhado
No Mês" as numeric(18, 2)) as vl_empenhado_mes,
        cast("Vr. Liquidado
No Mês" as numeric(18, 2)) as vl_liquidado_mes,
        cast("Vr. Pago
No Mês" as numeric(18, 2)) as vl_pago_mes
    from {{ ref('stg_pjf_despesa_mensal_consolidada') }}
)

select m.*
from marked as m
left join {{ ref('dim_tempo') }} as t_emp
    on m.sk_tempo_empenho = t_emp.sk_tempo
left join {{ ref('dim_tempo') }} as t_liq
    on m.sk_tempo_liquidacao = t_liq.sk_tempo
