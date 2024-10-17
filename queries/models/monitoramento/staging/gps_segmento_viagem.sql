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
    data = date_sub(date('{{ var("run_date") }}'), interval 1 day)
{% endset %}
{# {% set data_versao_efetiva = ref("subsidio_data_versao_efetiva") %} #}
{% set data_versao_efetiva = (
    "rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva"
) %}

{% if execute %}
    {% set data_versao_gtfs_query %}
        select format_date("%Y-%m-%d", feed_start_date) from {{ data_versao_efetiva }} where {{ incremental_filter }}
    {% endset %}

    {% set data_versao_gtfs = (
        run_query(data_versao_gtfs_query).columns[0].values()[0]
    ) %}
{% endif %}

with
    gps_viagem as (
        select id_viagem, shape_id, geo_point_gps
        from {{ ref("gps_viagem") }}
        where {{ incremental_filter }}
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
        where feed_start_date = '{{ data_versao_gtfs }}'
    ),
    gps_segmento as (
        select g.id_viagem, g.shape_id, s.id_segmento, count(*) as quantidade_gps
        from gps_viagem g
        join
            segmento s
            on g.shape_id = s.shape_id
            and st_intersects(s.buffer, g.geo_point_gps)
        group by 1, 2, 3
    ),
    viagem as (
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
            sentido
        from {{ ref("viagem_informada_monitoramento") }}
        where {{ incremental_filter }}
    ),
    viagem_segmento as (
        select
            v.data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            v.id_veiculo,
            v.trip_id,
            v.route_id,
            shape_id,
            s.id_segmento,
            s.indicador_segmento_desconsiderado,
            v.servico,
            v.sentido,
            s.feed_version,
            s.feed_start_date,
            s.feed_end_date
        from viagem v
        join segmento s using (shape_id)
    )
select
    v.data,
    id_viagem,
    v.datetime_partida,
    v.datetime_chegada,
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
    v.feed_end_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_segmento v
left join gps_segmento g using (id_viagem, shape_id, id_segmento)
