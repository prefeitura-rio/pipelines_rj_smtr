{{ config(alias="servicos_sentido") }}

with
    servicos_exclusivos_sabado as (
        select distinct servico
        from {{ ref("ordem_servico_gtfs") }}
        where tipo_dia = "Dia Útil" and viagens_planejadas = 0
    ),
    servicos as (
        select * except (versao_modelo, shape)
        from {{ ref("trips_gtfs") }} as t
        left join {{ ref("shapes_geom_gtfs") }} as s using (feed_start_date, shape_id)
        where
            (
                feed_start_date >= "2023-06-01"
                and (
                    trip_short_name not in (select * from servicos_exclusivos_sabado)
                    and (service_id like "U_R%" or service_id like "U_O%")
                )
                or (
                    trip_short_name in (select * from servicos_exclusivos_sabado)
                    and (service_id like "S_R%" or service_id like "S_O%")
                )
            )
            or (
                feed_start_date < "2023-06-01"
                and (
                    trip_short_name not in (select * from servicos_exclusivos_sabado)
                    or (
                        trip_short_name in (select * from servicos_exclusivos_sabado)
                        and service_id = "S"
                    )
                )
            )
            and shape_distance is not null
    ),
    servicos_rn as (
        select
            *,
            row_number() over (
                partition by feed_start_date, trip_short_name, direction_id
                order by trip_short_name, service_id, shape_id, direction_id
            ) as rn
        from servicos
    ),
    servicos_filtrada as (select * except (rn) from servicos_rn where rn = 1),
    servicos_potencialmente_circulares as (
        select
            feed_start_date, trip_short_name, count(distinct direction_id) as q_direcoes
        from servicos_filtrada
        group by 1, 2
        having count(distinct direction_id) = 1
    )
select
    feed_start_date,
    trip_short_name as servico,
    case
        when q_direcoes = 1 and st_distance(start_pt, end_pt) <= 50
        then "C"
        when direction_id = "0"
        then "I"
        when direction_id = "1"
        then "V"
    end as sentido
from servicos_filtrada as sf
left join
    servicos_potencialmente_circulares as spc using (feed_start_date, trip_short_name)
