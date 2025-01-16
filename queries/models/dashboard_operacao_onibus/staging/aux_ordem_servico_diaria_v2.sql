{{ config(materialized="ephemeral") }}


with
    feed_info as (select * from `rj-smtr`.`gtfs`.`feed_info`),
    ordem_servico_pivot as (
        select *
        from
            {{ ref("ordem_servico_gtfs") }} pivot (
                max(partidas_ida) as partidas_ida,
                max(partidas_volta) as partidas_volta,
                max(viagens_planejadas) as viagens_planejadas,
                max(distancia_total_planejada) as km for tipo_dia in (
                    'Dia Útil' as du,
                    'Ponto Facultativo' as pf,
                    'Sabado' as sab,
                    'Domingo' as dom
                )
            )
    ),
    subsidio_feed_start_date_efetiva as (
        select
            data,
            tipo_os,
            split(tipo_dia, " - ")[0] as tipo_dia,
            tipo_dia as tipo_dia_original
        from {{ ref("subsidio_data_versao_efetiva") }}
    ),
    ordem_servico_trips_shapes as (
        select distinct feed_start_date, servico, tipo_os, sentido
        from {{ ref("ordem_servico_trips_shapes_gtfs") }}
    )
select
    data,
    tipo_dia_original as tipo_dia,
    servico,
    vista,
    consorcio,
    sentido,
    case
        when sentido in ('I', 'C') and tipo_dia = "Dia Útil"
        then partidas_ida_du
        when sentido in ('I', 'C') and tipo_dia = "Ponto Facultativo"
        then partidas_ida_pf
        when sentido in ('I', 'C') and tipo_dia = "Sabado"
        then round(safe_divide((partidas_ida_du * km_sab), km_du))
        when sentido in ('I', 'C') and tipo_dia = "Domingo"
        then round(safe_divide((partidas_ida_du * km_dom), km_du))
        when sentido = "V" and tipo_dia = "Dia Útil"
        then partidas_volta_du
        when sentido = "V" and tipo_dia = "Ponto Facultativo"
        then partidas_volta_pf
        when sentido = "V" and tipo_dia = "Sabado"
        then round(safe_divide((partidas_volta_du * km_sab), km_du))
        when sentido = "V" and tipo_dia = "Domingo"
        then round(safe_divide((partidas_volta_du * km_dom), km_du))
    end as viagens_planejadas,
    horario_inicio as inicio_periodo,
    horario_fim as fim_periodo
from
    unnest(
        generate_date_array(
            (select min(feed_start_date) from feed_info),
            (select max(feed_end_date) from feed_info)
        )
    ) as data
left join feed_info as d on data between d.feed_start_date and d.feed_end_date
left join ordem_servico_pivot as o using (feed_start_date)
inner join subsidio_feed_start_date_efetiva as sd using (data, tipo_os)
left join ordem_servico_trips_shapes using (feed_start_date, servico, tipo_os)
where data >= "{{ var('data_inicio_trips_shapes') }}"
