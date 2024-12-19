{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="viagem_informada",
    )
}}


{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endset %}

{% set staging_viagem_informada_rioonibus = ref("staging_viagem_informada_rioonibus") %}
{% set staging_viagem_informada_brt = ref("staging_viagem_informada_brt") %}
{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            SELECT DISTINCT
                CONCAT("'", DATE(data_viagem), "'") AS data_viagem
            FROM
                {{ staging_viagem_informada_rioonibus }}
            WHERE
                {{ incremental_filter }}

            UNION DISTINCT

            SELECT DISTINCT
                CONCAT("'", DATE(data_viagem), "'") AS data_viagem
            FROM
                {{ staging_viagem_informada_brt }}
            WHERE
                {{ incremental_filter }}

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

        {% if partitions | length > 0 %}
            {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where data in ({{ partitions | join(", ") }})
            {% endset %}

            {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
        {% else %} {% set gtfs_feeds = [] %}
        {% endif %}
    {% endif %}

{% endif %}

with
    staging_rioonibus as (
        select
            data_viagem as data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            fornecedor as fonte_gps,
            datetime_processamento,
            timestamp_captura as datetime_captura
        from {{ staging_viagem_informada_rioonibus }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    staging_brt as (
        select
            data_viagem as data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            "brt" as fonte_gps,
            datetime_processamento,
            timestamp_captura as datetime_captura
        from {{ staging_viagem_informada_brt }}
        where
            {% if is_incremental() %} {{ incremental_filter }} and {% endif %}
            datetime_processamento >= "2024-09-10 13:00:00"
    ),
    staging_union as (
        select *
        from staging_rioonibus

        union all

        select *
        from staging_brt
    ),
    staging as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            case
                when sentido = 'I'
                then 'Ida'
                when sentido = 'V'
                then 'Volta'
                when sentido = 'C'
                then 'Circular'
                else sentido
            end as sentido,
            fonte_gps,
            datetime_processamento,
            datetime_captura
        from staging_union
    ),
    complete_partitions as (
        select *, 0 as priority
        from staging

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select * except (modo, versao, datetime_ultima_atualizacao), 1 as priority
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    deduplicado as (
        select * except (rn, priority)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_viagem order by datetime_captura desc, priority
                    ) as rn
                from complete_partitions
            )
        where rn = 1
    ),
    calendario as (
        select *
        from {{ calendario }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    routes as (
        select *
        from {{ ref("routes_gtfs") }}
        {# from `rj-smtr.gtfs.routes` #}
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    viagem_modo as (
        select
            data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            case
                when v.fonte_gps = 'brt'
                then 'BRT'
                when r.route_type = '200'
                then 'Ônibus Executivo'
                when r.route_type = '700'
                then 'Ônibus SPPO'
            end as modo,
            v.id_veiculo,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.servico,
            v.sentido,
            v.fonte_gps,
            v.datetime_processamento,
            v.datetime_captura
        from deduplicado v
        join calendario c using (data)
        left join routes r using (route_id, feed_start_date, feed_version)
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_modo
