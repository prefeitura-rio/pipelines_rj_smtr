{{ config(materialized="ephemeral") }}

with
    registros as (
        select id_veiculo, servico, data, posicao_veiculo_geo, datetime_gps
        from {{ ref("aux_gps_filtrada") }}
    ),
    shapes as (
        select distinct data, servico, feed_version, feed_start_date, route_id, shape_id
        {# from {{ ref("viagem_planejada_planejamento") }} #}
        from `rj-smtr`.`planejamento`.`viagem_planejada`
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
    ),
    intersec as (
        select
            r.*,
            s.feed_version,
            s.route_id,
            s.shape_id,
            st_dwithin(
                sg.shape, r.posicao_veiculo_geo, {{ var("buffer_segmento_metros") }}
            ) as indicador_intersecao
        from registros r
        left join shapes s using (servico, data)
        left join
            {# {{ ref("shapes_geom_gtfs") }} sg using ( #}
            `rj-smtr`.`gtfs`.`shapes_geom` sg using (
                feed_version, feed_start_date, shape_id
            )
    ),
    indicador as (
        select
            *,
            count(case when indicador_intersecao then 1 end) over (
                partition by id_veiculo
                order by
                    unix_seconds(timestamp(datetime_gps))
                    range
                    between {{ var("intervalo_max_desvio_segundos") }} preceding
                    and current row
            )
            >= 1 as indicador_trajeto_correto
        from intersec
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    route_id,
    logical_or(indicador_trajeto_correto) as indicador_trajeto_correto
from indicador
group by id_veiculo, servico, route_id, data, datetime_gps
