with source as (
    select * from {{ source('staging', 'stn_ementario_natureza_receita') }}
    qualify dt_atualizacao = max(dt_atualizacao) over (partition by nm_arquivo)
),

normalized as (
    select
        regexp_replace(cast(nr as varchar), '[^0-9]', '', 'g') as cd_natureza_receita,
        case
            when
                right(trim(cast(especificação as varchar)), 1) = '.'
                then left(
                    trim(cast(especificação as varchar)),
                    length(trim(cast(especificação as varchar))) - 1
                )
            else trim(cast(especificação as varchar))
        end as ds_natureza_receita,
        cast(nu_ano_referencia as int) as nu_ano_referencia,
        status as ds_status
    from source
),

filtered as (
    select *
    from normalized
    where
        cd_natureza_receita is not null
        and cd_natureza_receita <> ''
        and ds_status <> 'Excluído'
)

select *
from filtered
