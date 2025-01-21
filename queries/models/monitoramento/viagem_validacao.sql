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
            countif(id_segmento is not null) as quantidade_segmentos_verificados,
            countif(quantidade_gps > 0) as quantidade_segmentos_validos,
            max(indicador_servico_divergente) as indicador_servico_divergente,
            max(id_segmento is null) as indicador_shape_invalido,
            service_ids,
            tipo_dia,
            feed_version,
            feed_start_date
        from {{ ref("gps_segmento_viagem") }}
        where
            (
                not indicador_segmento_desconsiderado
                or indicador_segmento_desconsiderado is null
            )
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
            safe_divide(
                quantidade_segmentos_validos, quantidade_segmentos_verificados
            ) as indice_validacao,
            indicador_servico_divergente,
            indicador_shape_invalido,
            service_ids,
            tipo_dia,
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
    servicos_planejados_gtfs as (
        select
            i.*,
            (
                select count(*)
                from unnest(i.service_ids) as service_id
                join unnest(t.service_ids) as service_id using (service_id)
            )
            > 0 as indicador_servico_planejado_gtfs
        from indice i
        left join trips t using (feed_start_date, feed_version, route_id)
    ),
    viagem_planejada as (
        select *
        from {{ ref("viagem_planejada") }}
        {# from `rj-smtr.projeto_subsidio_sppo.viagem_planejada` #}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by data, servico, sentido, faixa_horaria_inicio
                order by distancia_planejada desc
            )
            = 1
    ),
    servicos_planejados_os as (
        select
            sp.*,
            case
                when
                    vp.distancia_total_planejada is not null
                    and vp.distancia_total_planejada > 0
                then true
                when
                    (
                        vp.distancia_total_planejada is not null
                        and vp.distancia_total_planejada <= 0
                    )
                    or (
                        vp.distancia_total_planejada is null and sp.modo = "Ônibus SPPO"
                    )
                then false
            end as indicador_servico_planejado_os
        from servicos_planejados_gtfs sp
        left join
            viagem_planejada vp
            on vp.servico = sp.servico
            and vp.sentido = sp.sentido
            and vp.data = sp.data
            and sp.datetime_partida between faixa_horaria_inicio and faixa_horaria_fim
    )
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
    indice_validacao,
    indice_validacao >= {{ var("parametro_validacao") }} as indicador_trajeto_valido,
    indicador_servico_planejado_gtfs,
    indicador_servico_planejado_os,
    indicador_servico_divergente,
    indicador_shape_invalido,
    (
        shape_id is not null
        and route_id is not null
        and not indicador_shape_invalido
        and indice_validacao >= {{ var("parametro_validacao") }}
        and indicador_servico_planejado_gtfs
        and ifnull(indicador_servico_planejado_os, true)
    ) as indicador_viagem_valida,
    {{ var("parametro_validacao") }} as parametro_validacao,
    tipo_dia,
    feed_version,
    feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from servicos_planejados_os
