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
            sentido,
            sum(partidas) as viagens_planejadas,
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        group by all
    )
select
    data,
    tipo_dia,
    servico,
    vista,
    consorcio,
    sentido,
    viagens_planejadas,
from calendario
left join viagens using (feed_start_date, tipo_dia, tipo_os)
left join ordem_servico_trips_shapes using (feed_start_date, servico, tipo_os, sentido)
