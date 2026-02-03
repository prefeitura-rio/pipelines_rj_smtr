{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        }
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
    {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{% endif %}

with
    calendario as (
        select data, tipo_dia, service_ids
        from {{ calendario }}
        where {{ incremental_filter }}
    ),
    viagem_planejada as (
        select
            data,
            id_viagem,
            c.tipo_dia,
            feed_version,
            feed_start_date,
            servico,
            sentido,
            first_value(datetime_partida) over (
                partition by data, servico, sentido
                order by
                    feed_start_date desc,
                    datetime_ultima_atualizacao desc,
                    datetime_partida
            ) as primeiro_horario,
            modo,
            trip_id,
            route_id,
            shape_id,
            first_value(datetime_partida) over (
                partition by data, servico, sentido
                order by
                    feed_start_date desc,
                    datetime_ultima_atualizacao desc,
                    datetime_partida desc
            ) as ultimo_horario
        from {{ ref("viagem_planejada_planejamento") }}
        left join calendario as c using (data)
        where
            {{ incremental_filter }}
            and modo = "BRT"
            and service_id in unnest(service_ids)
        qualify
            row_number() over (
                partition by data, servico, sentido, id_viagem
                order by
                    feed_start_date desc,
                    datetime_ultima_atualizacao desc,
                    datetime_partida
            )
            = 1
    ),
    shape_segmento as (
        select feed_start_date, shape_id, sum(comprimento_segmento) / 1000 as extensao
        from {{ ref("segmento_shape") }}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        group by all
    ),
    viagem_planejada_extensao as (
        select *
        from viagem_planejada
        left join shape_segmento using (feed_start_date, shape_id)
    )
select
    data,
    feed_version,
    feed_start_date,
    tipo_dia,
    "Regular" as tipo_os,
    servico,
    "MobiRio" as consorcio,
    sentido,
    sum(extensao) as extensao,
    count(distinct id_viagem) as viagens,
    primeiro_horario as faixa_horaria_inicio,
    ultimo_horario as faixa_horaria_fim,
    modo,
    any_value(
        struct(
            primeiro_horario,
            ultimo_horario,
            trip_id,
            route_id,
            shape_id,
            false as indicador_trajeto_alternativo
        )
    ) as trip_info,
    any_value(
        struct(null as trip_id, null as shape_id, null as evento, null as extensao)
    ) as trajetos_alternativos
from viagem_planejada_extensao
group by all
