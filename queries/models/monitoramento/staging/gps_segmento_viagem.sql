{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}

{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
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
    gps_viagem as (
        select
            data,
            gv.id_viagem,
            gv.shape_id,
            gv.geo_point_gps,
            c.feed_version,
            c.feed_start_date
        from {{ ref("gps_viagem") }} gv
        join calendario c using (data)
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    segmento as (
        select
            feed_version,
            feed_start_date,
            feed_end_date,
            shape_id,
            id_segmento,
            buffer,
            indicador_segmento_desconsiderado
        from {{ ref("segmento_shape") }}
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    gps_segmento as (
        select g.id_viagem, g.shape_id, s.id_segmento, count(*) as quantidade_gps
        from gps_viagem g
        join
            segmento s
            on g.feed_version = s.feed_version
            and g.shape_id = s.shape_id
            and st_intersects(s.buffer, g.geo_point_gps)
        group by 1, 2, 3
    ),
    viagem as (
        select
            data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            v.modo,
            v.id_veiculo,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.servico,
            v.sentido,
            c.tipo_dia,
            c.feed_start_date,
            c.feed_version
        from {{ ref("viagem_informada_monitoramento") }} v
        join calendario c using (data)
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    viagem_segmento as (
        select
            v.data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            v.modo,
            v.id_veiculo,
            v.trip_id,
            v.route_id,
            shape_id,
            s.id_segmento,
            s.indicador_segmento_desconsiderado,
            v.servico,
            v.sentido,
            v.tipo_dia,
            feed_version,
            feed_start_date
        from viagem v
        join segmento s using (shape_id, feed_version, feed_start_date)
    )
select
    v.data,
    id_viagem,
    v.datetime_partida,
    v.datetime_chegada,
    v.modo,
    v.id_veiculo,
    v.trip_id,
    v.route_id,
    shape_id,
    id_segmento,
    v.indicador_segmento_desconsiderado,
    v.servico,
    v.sentido,
    ifnull(g.quantidade_gps, 0) as quantidade_gps,
    v.feed_version,
    v.feed_start_date,
    v.tipo_dia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_segmento v
left join gps_segmento g using (id_viagem, shape_id, id_segmento)
