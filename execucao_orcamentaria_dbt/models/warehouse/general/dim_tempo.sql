with
dates_raw as (
        {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="date_add(current_date(), interval 1 year)"
        )
    }}
),

days_info as (
    select
        cast(date_day as date) as date_day,
        {{ dbt_utils.generate_surrogate_key(['cast(date_day as varchar)']) }} as sk_tempo,
        cast(date_part('isodow', date_day) as int) as nu_dia_semana,
        cast(extract(month from date_day) as int) as nu_mes,
        cast(extract(quarter from date_day) as int) as nu_trimestre,
        cast(extract(doy from date_day) as int) as nu_dia_ano,
        cast(extract(year from date_day) as int) as nu_ano,
        strftime(date_day, '%B') as ds_mes_ingles
    from dates_raw
),

days_named as (
    select
        sk_tempo,
        cast(date_day as date) as dt_dia,
        nu_dia_semana,
        case nu_dia_semana
            when 1 then 'Segunda-feira'
            when 2 then 'Terça-feira'
            when 3 then 'Quarta-feira'
            when 4 then 'Quinta-feira'
            when 5 then 'Sexta-feira'
            when 6 then 'Sábado'
            else 'Domingo'
        end as nm_dia_semana,
        nu_mes,
        ds_mes_ingles,
        case nu_mes
            when 1 then 'Janeiro'
            when 2 then 'Fevereiro'
            when 3 then 'Março'
            when 4 then 'Abril'
            when 5 then 'Maio'
            when 6 then 'Junho'
            when 7 then 'Julho'
            when 8 then 'Agosto'
            when 9 then 'Setembro'
            when 10 then 'Outubro'
            when 11 then 'Novembro'
            else 'Dezembro'
        end as nm_mes,
        case nu_mes
            when 1 then 'Jan'
            when 2 then 'Fev'
            when 3 then 'Mar'
            when 4 then 'Abr'
            when 5 then 'Mai'
            when 6 then 'Jun'
            when 7 then 'Jul'
            when 8 then 'Ago'
            when 9 then 'Set'
            when 10 then 'Out'
            when 11 then 'Nov'
            else 'Dez'
        end as sg_mes,
        nu_trimestre,
        case nu_trimestre
            when 1 then '1º Trimestre'
            when 2 then '2º Trimestre'
            when 3 then '3º Trimestre'
            else '4º Trimestre'
        end as ds_trimestre,
        case
            when nu_trimestre in (1, 2) then 1
            else 2
        end as nu_semestre,
        case
            when nu_trimestre in (1, 2) then '1º Semestre'
            else '2º Semestre'
        end as ds_semestre,
        nu_dia_ano,
        nu_ano
    from days_info
)

select
    sk_tempo,
    dt_dia,
    nu_dia_semana,
    nm_dia_semana,
    nu_mes,
    ds_mes_ingles,
    nm_mes,
    sg_mes,
    nu_trimestre,
    ds_trimestre,
    nu_semestre,
    ds_semestre,
    nu_dia_ano,
    nu_ano
from days_named
