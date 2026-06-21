with source as (
    select * from {{ source('staging', 'pjf_loa_subfuncao') }}
),

normalized as (
    select
        cast(nu_ano_referencia as bigint) as nu_ano_referencia,
        lpad(trim(cd_subfuncao), 3, '0') as cd_subfuncao,
        trim(ds_subfuncao) as ds_subfuncao,
        nm_arquivo,
        dt_atualizacao
    from source
    where
        cd_subfuncao is not null
        and trim(cd_subfuncao) <> ''
        and ds_subfuncao is not null
        and trim(ds_subfuncao) <> ''
)

select *
from normalized
qualify row_number() over (
    partition by cd_subfuncao
    order by dt_atualizacao desc
) = 1
