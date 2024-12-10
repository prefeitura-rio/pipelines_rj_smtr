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
    trips_dia as (
        select *
        from {{ ref("aux_trips_dia") }}
        where
            {% if is_incremental() %}
                feed_start_date in ({{ gtfs_feeds | join(", ") }})
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% endif %}
    ),
    frequencies_tratada as (
        select *
        from {{ ref("aux_frequencies_horario_tratado") }}
        where
            {% if is_incremental() %} feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% else %} feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% endif %}
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
    os_trajetos_alternativos as (
        select *
        {# from `rj-smtr.gtfs.ordem_servico_trajeto_alternativo` #}
        from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
        where
            {% if is_incremental() %} feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% else %} feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
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
        where td.trip_id not in (select trip_id from frequencies_tratada)
        group by 1, 2, 3
    ),
    viagens as (
        select
            tfd.*,
            datetime(partida, "America/Sao_Paulo") as datetime_partida,
            ta.trajetos_alternativos
        from
            trips_frequences_dia tfd,
            unnest(
                generate_timestamp_array(
                    start_timestamp,
                    timestamp_sub(end_timestamp, interval 1 second),
                    interval headway_secs second
                )
            ) as partida
        left join trips_alternativas ta using (data, servico, direction_id)
    ),
    ordem_servico_tratada as (
        select *
        from {{ ref("aux_ordem_servico_horario_tratado") }}
        where
            {% if is_incremental() %} feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% else %} feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% endif %}
    ),
    viagem_os as (
        -- filtra viagens fora do horario de inicio e fim e em dias não previstos na OS
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
    servico_circular as (
        select feed_start_date, feed_version, shape_id
        {# from `rj-smtr.planejamento.shapes_geom` #}
        from {{ ref("shapes_geom_planejamento") }}
        where
            {% if is_incremental() %} feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% else %} feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% endif %}
            and round(st_y(start_pt), 4) = round(st_y(end_pt), 4)
            and round(st_x(start_pt), 4) = round(st_x(end_pt), 4)
    ),
    viagem_planejada as (
        select
            date(datetime_partida) as data,
            datetime_partida,
            modo,
            service_id,
            trip_id,
            route_id,
            shape_id,
            servico,
            case
                when c.shape_id is not null
                then "C"
                when direction_id = '0'
                then "I"
                else "V"
            end as sentido,
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
        from viagem_os v
        left join servico_circular c using (shape_id, feed_version, feed_start_date)
    ),
    viagem_planejada_id as (
        select
            *,
            concat(
                servico,
                "_",
                sentido,
                "_",
                shape_id,
                "_",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem
        from viagem_planejada
    )
select data, id_viagem, * except (data, id_viagem, rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_viagem order by data_referencia desc
            ) as rn
        from viagem_planejada_id
    )
where rn = 1
