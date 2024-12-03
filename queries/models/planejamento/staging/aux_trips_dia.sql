{{ config(materialized="ephemeral") }}


with
    routes as (
        select
            *,
            case
                when agency_id in ("22005", "22002", "22004", "22003")
                then "Ônibus"
                when agency_id = "20001"
                then "BRT"
            end as modo
        from `rj-smtr.gtfs.routes`
    {# from {{ ref("routes_gtfs") }} #}
    )
select
    c.data,
    t.trip_id,
    r.modo,
    t.route_id,
    t.service_id,
    r.route_short_name as servico,
    t.direction_id,
    t.shape_id,
    c.tipo_dia,
    c.subtipo_dia,
    c.tipo_os,
    t.feed_version,
    t.feed_start_date,
    regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
{# from `rj-smtr.planejamento.calendario` c #}
from {{ ref("calendario") }} c
{# join `rj-smtr.gtfs.trips` t using (feed_start_date, feed_version) #}
join {{ ref("trips_gtfs") }} t using (feed_start_date, feed_version)
join routes r using (feed_start_date, feed_version, route_id)
where t.service_id in unnest(c.service_ids)
