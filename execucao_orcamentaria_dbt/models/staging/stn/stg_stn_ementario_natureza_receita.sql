with source as (
    select * from {{ source('staging', 'stn_ementario_natureza_receita') }}
),
normalized as (
    select
        case
            when length(regexp_replace(cd_natureza_receita, '[^0-9]', '', 'g')) >= 8
                then left(
                    regexp_replace(cd_natureza_receita, '[^0-9]', '', 'g'),
                    8
                )
            else regexp_replace(cd_natureza_receita, '[^0-9]', '', 'g')
        end as cd_natureza_receita,
        CASE
            WHEN right(trim(ds_natureza_receita), 1) = '.' THEN left(trim(ds_natureza_receita), length(trim(ds_natureza_receita)) - 1)
            ELSE trim(ds_natureza_receita)
        END AS ds_natureza_receita,
        cast(nu_ano_referencia as int) as nu_ano_referencia,
        nm_arquivo,
        dt_atualizacao
    from source
)
select *
from normalized
where cd_natureza_receita is not null
  and cd_natureza_receita <> ''
