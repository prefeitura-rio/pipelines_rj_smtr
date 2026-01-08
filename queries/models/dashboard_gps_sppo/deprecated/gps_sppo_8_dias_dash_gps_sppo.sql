{{ config(alias="gps_sppo_8_dias") }}

with
    planejado as (
        select distinct data, servico
        from {{ ref("viagem_planejada") }}
        where data between date_sub(current_date(), interval 8 day) and current_date()
    ),
    gps as (
        select
            id_veiculo,
            servico,
            latitude,
            longitude,
            date(timestamp_gps) as data,
            time(timestamp_gps) as hora,
            timestamp_gps
        from {{ ref("gps_sppo") }}
        where data between date_sub(current_date(), interval 8 day) and current_date()
    )
select gps.*
from gps
right join planejado using (data, servico)
