with snapshots as (
    select distinct
        sk_empenho,
        sk_unidade_administrativa,
        sk_tempo_empenho,
        sk_tempo_liquidacao,
        sk_fornecedor,
        sk_funcional_pragmatica,
        sk_natureza_despesa,
        sk_fonte_recurso,
        nu_ano_referencia,
        nu_mes_referencia,
        dt_liquidacao,
        vl_empenhado_mes,
        vl_liquidado_mes,
        vl_pago_mes
    from {{ ref('int_pjf_despesa_mensal_consolidada') }}
),

ranked as (
    select
        *,
        max(nu_ano_referencia * 100 + nu_mes_referencia) over (
            partition by
                sk_empenho,
                sk_unidade_administrativa,
                sk_tempo_empenho,
                sk_fornecedor,
                sk_funcional_pragmatica,
                sk_natureza_despesa,
                sk_fonte_recurso
        ) as nu_ref_max
    from snapshots
)

select
    sk_empenho,
    sk_unidade_administrativa,
    sk_tempo_empenho,
    arg_max(sk_tempo_liquidacao, dt_liquidacao) as sk_tempo_liquidacao,
    sk_fornecedor,
    sk_funcional_pragmatica,
    sk_natureza_despesa,
    sk_fonte_recurso,
    sum(vl_empenhado_mes) as vl_empenhado_mes,
    sum(vl_liquidado_mes) as vl_liquidado_mes,
    sum(vl_pago_mes) as vl_pago_mes
from ranked
where nu_ano_referencia * 100 + nu_mes_referencia = nu_ref_max
group by
    sk_empenho,
    sk_unidade_administrativa,
    sk_tempo_empenho,
    sk_fornecedor,
    sk_funcional_pragmatica,
    sk_natureza_despesa,
    sk_fonte_recurso
