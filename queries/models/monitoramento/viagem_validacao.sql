{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
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
    contagem as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            modo,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            count(*) as quantidade_segmentos_verificados,
            countif(quantidade_gps > 0) as quantidade_segmentos_validos,
            service_ids,
            tipo_dia,
            tipo_os,
            feed_version,
            feed_start_date
        from {{ ref("gps_segmento_viagem") }}
        where
            not indicador_segmento_desconsiderado
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
        group by
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            modo,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            service_ids,
            tipo_dia,
            tipo_os,
            feed_version,
            feed_start_date
    ),
    indice as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            modo,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            quantidade_segmentos_verificados,
            quantidade_segmentos_validos,
            quantidade_segmentos_validos
            / quantidade_segmentos_verificados as indice_validacao,
            service_ids,
            tipo_dia,
            tipo_os,
            feed_version,
            feed_start_date
        from contagem
    ),
    trips as (
        select distinct
            feed_start_date,
            feed_version,
            route_id,
            array_agg(service_id) as service_ids,
        from {{ ref("trips_gtfs") }}
        {# from `rj-smtr.gtfs.trips` #}
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
        group by 1, 2, 3
    ),
    servicos_planejados as (
        select
            i.*,
            (
                select count(*)
                from unnest(i.service_ids) as service_id
                join unnest(t.service_ids) as service_id using (service_id)
            )
            > 0 as indicador_servico_planejado
        from indice i
        left join trips t using (feed_start_date, feed_version, route_id)
    )
select
    s.data,
    s.id_viagem,
    s.datetime_partida,
    s.datetime_chegada,
    s.modo,
    s.id_veiculo,
    s.trip_id,
    s.route_id,
    s.shape_id,
    s.servico,
    s.sentido,
    s.quantidade_segmentos_verificados,
    s.quantidade_segmentos_validos,
    s.indice_validacao,
    s.indice_validacao >= {{ var("parametro_validacao") }} as indicador_trajeto_valido,
    s.indicador_servico_planejado,
    s.indice_validacao >= {{ var("parametro_validacao") }}
    and s.indicador_servico_planejado as indicador_viagem_valida,
    {{ var("parametro_validacao") }} as parametro_validacao,
    s.tipo_dia,
    s.tipo_os,
    s.feed_version,
    s.feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from servicos_planejados s
