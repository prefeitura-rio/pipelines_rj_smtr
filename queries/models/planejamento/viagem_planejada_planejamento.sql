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


{% set calendario = ref("calendario") %}

{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where
                data between date("{{ var('date_range_start') }}")
                and date("{{ var('date_range_end') }}")
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
            ) as start_time,
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
            ) as end_time
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
        {# from `rj-smtr.gtfs.routes` #}
        from {{ ref("routes_gtfs") }}
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
            r.route_short_name as servico,
            t.direction_id,
            t.shape_id,
            c.tipo_dia,
            c.subtipo_dia,
            c.tipo_os,
            t.feed_version,
            t.feed_start_date,
            regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
        from calendario c
        join trips t using (feed_start_date, feed_version)
        join routes r using (feed_start_date, feed_version, route_id)
        where t.service_id in unnest(c.service_ids)
    ),
    trips_frequences_dia as (
        select
            td.* except (evento),
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
        join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
    ),
    end_time_tratado as (
        select
            * except (end_timestamp),
            case
                when
                    end_timestamp > lead(start_timestamp) over (
                        partition by
                            feed_start_date, feed_version, data, servico, direction_id
                        order by start_timestamp
                    )
                then
                    lead(start_timestamp) over (
                        partition by
                            feed_start_date, feed_version, data, servico, direction_id
                        order by start_timestamp
                    )
                else end_timestamp
            end as end_timestamp
        from trips_frequences_dia
    ),
    os_trajetos_alternativos as (
        select *
        from `rj-smtr.gtfs.ordem_servico_trajeto_alternativo`
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}

    ),
    trips_alternativas as (
        select
            data,
            servico,
            direction_id,
            array_agg(
                struct(
                    td.trip_id as trip_id,
                    td.shape_id as shape_id,
                    evento as evento,
                    case
                        when td.direction_id = '0'
                        then os.extensao_ida
                        when td.direction_id = '1'
                        then os.extensao_volta
                    end as extensao
                )
            ) as trajetos_alternativos
        from trips_dia td
        join
            os_trajetos_alternativos os using (
                feed_start_date, feed_version, tipo_os, servico, evento
            )
        where td.trip_id not in (select trip_id from frequencies)
        group by 1, 2, 3
    ),
    viagens as (
        select
            ett.*,
            datetime(partida, "America/Sao_Paulo") as datetime_partida,
            ta.trajetos_alternativos
        from
            end_time_tratado ett,
            unnest(
                generate_timestamp_array(
                    start_timestamp,
                    timestamp_sub(end_timestamp, interval 1 second),
                    interval headway_secs second
                )
            ) as partida
        left join trips_alternativas ta using (data, servico, direction_id)
    ),
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            parse_time("%H:%M:%S", lpad(horario_inicio, 8, '0')) as horario_inicio,
            split(horario_fim, ":") horario_fim_parts
        from `rj-smtr.gtfs.ordem_servico`
        where
            {% if is_incremental() %} feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% else %} feed_start_date >= "2024-09-01"
            {% endif %}
    ),
    ordem_servico_tratada as (
        select
            * except (horario_fim_parts),
            div(cast(horario_fim_parts[0] as integer), 24) as dias_horario_fim,
            parse_time(
                "%H:%M:%S",
                concat(
                    lpad(
                        cast(
                            if(
                                cast(horario_fim_parts[0] as integer) >= 24,
                                cast(horario_fim_parts[0] as integer) - 24,
                                cast(horario_fim_parts[0] as integer)
                            ) as string
                        ),
                        2,
                        '0'
                    ),
                    ":",
                    horario_fim_parts[1],
                    ":",
                    horario_fim_parts[2]
                )
            ) as horario_fim,
        from ordem_servico

    ),
    viagem_os as (
        select
            v.*,
            case
                when v.direction_id = '0'
                then os.extensao_ida
                when v.direction_id = '1'
                then os.extensao_volta
            end as extensao
        from viagens v
        left join
            ordem_servico_tratada os using (
                feed_start_date, feed_version, tipo_os, tipo_dia, servico
            )
        where
            (os.distancia_total_planejada is null or os.distancia_total_planejada > 0)
            and (
                os.feed_start_date is null
                or v.datetime_partida
                between datetime(data, os.horario_inicio) and datetime(
                    date_add(data, interval os.dias_horario_fim day), os.horario_fim
                )
            )
    ),
    viagem_planejada as (
        select
            date(datetime_partida) as data,
            concat(
                servico,
                "_",
                direction_id,
                "_",
                shape_id,
                "_",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem,
            datetime_partida,
            modo,
            service_id,
            trip_id,
            route_id,
            shape_id,
            servico,
            case when direction_id = '0' then "Ida" else "Volta" end as sentido,
            extensao,
            trajetos_alternativos,
            data as data_referencia,
            tipo_dia,
            subtipo_dia,
            tipo_os,
            feed_version,
            feed_start_date,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from viagem_os
    )
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_viagem order by data_referencia desc
            ) as rn
        from viagem_planejada
    )
where rn = 1
