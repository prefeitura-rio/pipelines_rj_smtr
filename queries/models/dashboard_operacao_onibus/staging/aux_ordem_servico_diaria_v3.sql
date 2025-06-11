{{ config(materialized="ephemeral") }}


with
    calendario as (
        select data, tipo_dia, tipo_os, feed_start_date from {{ ref("calendario") }}
    ),
    ordem_servico_trips_shapes as (
        select distinct feed_start_date, servico, tipo_os, sentido
        from {{ ref("ordem_servico_trips_shapes_gtfs") }}
    ),
    viagens as (
        select distinct
            feed_start_date,
            tipo_os,
            servico,
            vista,
            consorcio,
            tipo_dia,
            viagens_dia as viagens_planejadas,
            horario_inicio as inicio_periodo,
            horario_fim as fim_periodo
        from {{ ref("ordem_servico_faixa_horaria") }}
    )
select
    data,
    tipo_dia,
    servico,
    vista,
    consorcio,
    sentido,
    viagens_planejadas,
    inicio_periodo,
    fim_periodo
from calendario
left join viagens using (feed_start_date, tipo_dia, tipo_os)
left join ordem_servico_trips_shapes using (feed_start_date, servico, tipo_os)
