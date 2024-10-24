{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        alias="viagem_planejada",
        incremental_strategy="merge",
        unique_key="id_viagem",
        incremental_predicates=[
            "DBT_INTERNAL_DEST.data between date('"
            + var("date_range_start")
            + "') and date_add(date('"
            + var("date_range_end")
            + "'), interval 1 day)"
        ],
    )
}}


{# {% set gtfs_feed_info = ref("feed_info_gtfs") %} #}
{% set gtfs_feed_info = "rj-smtr.gtfs.feed_info" %}
{% set calendario = ref("calendario") %}

{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_feeds_query %}
            select concat("'", feed_start_date, "'") as feed_start_date
            from {{ gtfs_feed_info }}
            where
                feed_start_date <= date("{{ var('date_range_end') }}")
                and (feed_end_date IS NULL OR feed_end_date >= date("{{ var('date_range_end') }}"))

            union distinct

            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where
                data between date("{{ var('date_range_start') }}")
                and date("{{ var('date_range_end') }}")
                and feed_start_date is not null
        {% endset %}

        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    calendario as (
        select *
        from {{ calendario }}
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        {% endif %}
    ),
    trips as (
        select *
        from `rj-smtr.gtfs.trips`
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    frequencies as (
        select
            *,
            split(start_time, ":") as start_time_parts,
            split(end_time, ":") as end_time_parts,
        from `rj-smtr.gtfs.frequencies`
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    frequencies_tratada as (
        select
            * except (start_time_parts, end_time_parts, start_time, end_time),
            div(cast(start_time_parts[0] as integer), 24) days_to_add_start,
            div(cast(end_time_parts[0] as integer), 24) days_to_add_end,
            concat(
                lpad(
                    cast(
                        if(
                            cast(start_time_parts[0] as integer) >= 24,
                            cast(start_time_parts[0] as integer) - 24,
                            cast(start_time_parts[0] as integer)
                        ) as string
                    ),
                    2,
                    '0'
                ),
                ":",
                start_time_parts[1],
                ":",
                start_time_parts[2]
            ) start_time,
            concat(
                lpad(
                    cast(
                        if(
                            cast(end_time_parts[0] as integer) >= 24,
                            cast(end_time_parts[0] as integer) - 24,
                            cast(end_time_parts[0] as integer)
                        ) as string
                    ),
                    2,
                    '0'
                ),
                ":",
                end_time_parts[1],
                ":",
                end_time_parts[2]
            ) end_time
        from frequencies
    ),
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
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    trips_dia as (
        select
            c.data,
            t.trip_id,
            r.modo,
            t.route_id,
            t.service_id,
            r.route_short_name,
            t.direction_id,
            t.shape_id,
            c.tipo_dia,
            c.subtipo_dia,
            c.tipo_os,
            t.feed_version,
            t.feed_start_date
        from calendario c
        join trips t using (feed_start_date)
        join routes r using (feed_start_date, route_id)
        where t.service_id in unnest(c.service_ids)
    ),
    trips_frequences_dia as (
        select
            td.*,
            timestamp(
                concat(
                    cast(date_add(data, interval f.days_to_add_start day) as string),
                    ' ',
                    f.start_time
                ),
                "America/Sao_Paulo"
            ) as start_timestamp,
            timestamp(
                concat(
                    cast(date_add(data, interval f.days_to_add_end day) as string),
                    ' ',
                    f.end_time
                ),
                "America/Sao_Paulo"
            ) as end_timestamp,
            f.headway_secs
        from trips_dia td
        join frequencies_tratada f using (feed_start_date, trip_id)
    ),
    viagens as (
        select *, datetime(partida, "America/Sao_Paulo") as datetime_partida
        from
            trips_frequences_dia,
            unnest(
                generate_timestamp_array(
                    start_timestamp,
                    timestamp_sub(end_timestamp, interval 1 second),
                    interval headway_secs second
                )
            ) as partida
    )
select
    date(datetime_partida) as data,
    concat(
        route_short_name,
        "_",
        direction_id,
        "_",
        shape_id,
        "_",
        format_timestamp("%Y%m%d%H%M%S", partida, "America/Sao_Paulo")
    ) as id_viagem,
    datetime_partida,
    modo,
    service_id,
    trip_id,
    route_id,
    shape_id,
    route_short_name as servico,
    case when direction_id = '0' then "Ida" else "Volta" end as sentido,
    tipo_dia,
    subtipo_dia,
    tipo_os,
    feed_version,
    feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagens
