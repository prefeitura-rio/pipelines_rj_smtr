{{ config(alias="ordem_servico_diaria") }}

with
    feed_start_date as (
        select
            feed_start_date,
            feed_start_date as data_inicio,
            coalesce(
                date_sub(
                    lead(feed_start_date) over (order by feed_start_date),
                    interval 1 day
                ),
                last_day(feed_start_date, month)
            ) as data_fim
        from (select distinct feed_start_date, from {{ ref("ordem_servico_gtfs") }})
    ),
    ordem_servico_pivot as (
        select *
        from
            {{ ref("ordem_servico_gtfs") }} pivot (
                max(partidas_ida) as partidas_ida,
                max(partidas_volta) as partidas_volta,
                max(viagens_planejadas) as viagens_planejadas,
                max(distancia_total_planejada) as km for
                tipo_dia in (
                    'Dia Útil' as du,
                    'Ponto Facultativo' as pf,
                    'Sabado' as sab,
                    'Domingo' as dom
                )
            )
    ),
    subsidio_feed_start_date_efetiva as (
        select
            data, split(tipo_dia, " - ")[0] as tipo_dia, tipo_dia as tipo_dia_original
        from {{ ref("subsidio_data_versao_efetiva") }}
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
            (select min(data_inicio) from feed_start_date),
            (select max(data_fim) from feed_start_date)
        )
    ) as data
left join feed_start_date as d on data between d.data_inicio and d.data_fim
left join subsidio_feed_start_date_efetiva as sd using (data)
left join ordem_servico_pivot as o using (feed_start_date)
left join {{ ref("servicos_sentido_gtfs") }} using (feed_start_date, servico)
