{%- set sk_tempo_empenho_col %}
strftime(try_cast("Data do Empenho" as date), '%Y-%m-%d')
{%- endset %}
{%- set sk_tempo_liq_col %}
strftime(try_cast("Data da Liquidação" as date), '%Y-%m-%d')
{%- endset %}
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
                    {{ dbt_utils.generate_surrogate_key([sk_tempo_empenho_col | trim]) }}
        end as sk_tempo_empenho,
        case
            when try_cast("Data da Liquidação" as date) is not null
                then
                    {{ dbt_utils.generate_surrogate_key([sk_tempo_liq_col | trim]) }}
        end as sk_tempo_liquidacao,
        {{ dbt_utils.generate_surrogate_key(['"CPF/CNPJ"']) }} as sk_fornecedor,

        {{ dbt_utils.generate_surrogate_key(['"Funcional Programática"']) }}
            as sk_funcional_pragmatica,
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
)

select m.*
from marked as m
left join {{ ref('dim_tempo') }} as t_emp
    on m.sk_tempo_empenho = t_emp.sk_tempo
left join {{ ref('dim_tempo') }} as t_liq
    on m.sk_tempo_liquidacao = t_liq.sk_tempo
