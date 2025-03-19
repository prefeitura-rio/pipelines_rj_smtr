{% if var("tipo_materializacao") == "monitoramento" %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
            schema="monitoramento_interno_teste",
        )
    }}
{% else %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}
{% endif %}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{# {% set calendario = ref("calendario") %} #}
{% set calendario = "rj-smtr.planejamento.calendario" %}
{% if execute %}
    {# {% if is_incremental() %} #}
    {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{# {% endif %} #}
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
            indicador_viagem_sobreposta,
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
            {# {% if is_incremental() %}  #}
            and {{ incremental_filter }}
        {# {% endif %} #}
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
            indicador_viagem_sobreposta,
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
            indicador_viagem_sobreposta,
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
        {# from {{ ref("trips_gtfs") }} #}
        from `rj-smtr.gtfs.trips`
        {# {% if is_incremental() %} #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {# {% endif %} #}
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
    servico_planejado as (
        select *
        {# ,
            sum(quilometragem) over (
                partition by data, servico, faixa_horaria_inicio
            ) as distancia_total_planejada #}
        from {{ ref("servico_planejado") }}
        {# {% if is_incremental() %}  #}
        where {{ incremental_filter }}
    {# {% endif %} #}
    ),
    servicos_planejados_os as (
        select
            spg.*,
            sp.extensao as distancia_planejada,
            sp.indicador_trajeto_alternativo,
            -- fmt: off
            sp.extensao*60/(datetime_diff(datetime_chegada, datetime_partida, minute) + 1) as velocidade_media,
            -- fmt: on
            case
                when sp.quilometragem is not null and sp.quilometragem > 0
                then true
                when
                    (sp.quilometragem is not null and sp.quilometragem <= 0)
                    or (sp.quilometragem is null and spg.modo = "Ônibus SPPO")
                then false
            end as indicador_servico_planejado_os
        from servicos_planejados_gtfs spg
        left join
            servico_planejado sp
            on sp.servico = spg.servico
            and sp.sentido = spg.sentido
            and sp.data = spg.data
            and spg.datetime_partida between faixa_horaria_inicio and faixa_horaria_fim
    ),
    viagens_velocidade_media as (
        select
            *,
            velocidade_media
            >= {{ var("conformidade_velocidade_min") }}
            as indicador_acima_velocidade_max
        from servicos_planejados_os
    ),
    viagens as (
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
            distancia_planejada,
            velocidade_media,
            quantidade_segmentos_verificados,
            quantidade_segmentos_validos,
            indice_validacao,
            indicador_viagem_sobreposta,
            -- fmt: off
            indice_validacao >= {{ var("parametro_validacao") }} as indicador_trajeto_valido,
            -- fmt: on
            indicador_servico_planejado_gtfs,
            indicador_servico_planejado_os,
            indicador_servico_divergente,
            indicador_shape_invalido,
            indicador_trajeto_alternativo,
            indicador_acima_velocidade_max,
            (
                shape_id is not null
                and route_id is not null
                and not indicador_shape_invalido
                and indice_validacao >= {{ var("parametro_validacao") }}
                and indicador_servico_planejado_gtfs
                and not indicador_viagem_sobreposta
                and not indicador_acima_velocidade_max
                and ifnull(indicador_servico_planejado_os, true)
            ) as indicador_viagem_valida,
            {{ var("parametro_validacao") }} as parametro_validacao,
            tipo_dia,
            feed_version,
            feed_start_date,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from viagens_velocidade_media
    ),
    filtro_desvio as (
        select *
        from viagens
        qualify
            row_number() over (
                partition by id_veiculo, datetime_partida, datetime_chegada
                order by
                    indice_validacao desc,
                    indicador_trajeto_alternativo,
                    distancia_planejada desc
            )
            = 1
    ),
    filtro_partida as (
        select *
        from filtro_desvio
        qualify
            row_number() over (
                partition by id_veiculo, datetime_partida
                order by distancia_planejada desc
            )
            = 1
    ),
    filtro_chegada as (
        select *
        from filtro_partida
        qualify
            row_number() over (
                partition by id_veiculo, datetime_chegada
                order by distancia_planejada desc
            )
            = 1
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
    distancia_planejada,
    velocidade_media,
    quantidade_segmentos_verificados,
    quantidade_segmentos_validos,
    indice_validacao,
    indicador_viagem_sobreposta,
    indicador_trajeto_valido,
    indicador_servico_planejado_gtfs,
    indicador_servico_planejado_os,
    indicador_servico_divergente,
    indicador_shape_invalido,
    indicador_trajeto_alternativo,
    indicador_acima_velocidade_max,
    indicador_viagem_valida,
    parametro_validacao,
    tipo_dia,
    feed_version,
    feed_start_date,
    versao,
    datetime_ultima_atualizacao
{% if var("tipo_materializacao") == "monitoramento" %} from filtro_chegada
{% else %} from viagens
{% endif %}
