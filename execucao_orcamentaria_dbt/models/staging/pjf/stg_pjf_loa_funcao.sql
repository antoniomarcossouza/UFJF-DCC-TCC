with source as (
    select * from {{ source('staging', 'pjf_loa_funcao') }}
),

normalized as (
    select
        cast(nu_ano_referencia as bigint) as nu_ano_referencia,
        lpad(trim(cd_funcao), 2, '0') as cd_funcao,
        trim(ds_funcao) as ds_funcao,
        nm_arquivo,
        dt_atualizacao
    from source
    where
        cd_funcao is not null
        and trim(cd_funcao) <> ''
        and ds_funcao is not null
        and trim(ds_funcao) <> ''
)

select *
from normalized
qualify row_number() over (
    partition by cd_funcao
    order by dt_atualizacao desc
) = 1
