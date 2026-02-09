{% if var("tipo_materializacao") == "monitoramento" %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
            schema="monitoramento_interno",
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
        date_sub(date('{{ var("date_range_start") }}'), interval 1 day)
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute %}
    {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
        {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
        {% endset %}
        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    /*
    Agregação para cálculo da quantidade de segmentos verificados, válidos e tolerados por viagem
    */
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
            countif(id_segmento is not null) as quantidade_segmentos_considerados,
            countif(quantidade_gps > 0) as quantidade_segmentos_validos,
            round(
                countif(id_segmento is not null)
                * safe_cast((1 - {{ var("parametro_validacao") }}) as numeric)
            ) as quantidade_segmentos_tolerados,
            max(indicador_servico_divergente) as indicador_servico_divergente,
            max(id_segmento is null) as indicador_shape_invalido,
            service_ids,
            tipo_dia,
            feed_version,
            feed_start_date,
            datetime_captura_viagem
        from {{ ref("gps_segmento_viagem") }}
        {# from `rj-smtr`.`monitoramento_staging`.`gps_segmento_viagem` #}
        where
            (
                not indicador_segmento_desconsiderado
                or indicador_segmento_desconsiderado is null
            )
            {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
                and {{ incremental_filter }}
            {% endif %}
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
            feed_start_date,
            datetime_captura_viagem
    ),
    /*
    Calcula o índice de validação, quantidade mínima de segmentos necessários e indicador de campos obrigatórios por viagem
    */
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
            quantidade_segmentos_considerados,
            quantidade_segmentos_validos,
            case
                when quantidade_segmentos_tolerados >= 1
                then quantidade_segmentos_considerados - quantidade_segmentos_tolerados
                else abs(quantidade_segmentos_considerados - 1)
            end as quantidade_segmentos_necessarios,
            safe_divide(
                quantidade_segmentos_validos, quantidade_segmentos_considerados
            ) as indice_validacao,
            indicador_servico_divergente,
            indicador_shape_invalido,
            (
                id_viagem is not null
                and datetime_partida is not null
                and datetime_chegada is not null
                and datetime_chegada > datetime_partida
                and shape_id is not null
                and route_id is not null
                and id_veiculo is not null
                and id_veiculo != ""
            ) as indicador_campos_obrigatorios,
            service_ids,
            tipo_dia,
            feed_version,
            feed_start_date,
            datetime_captura_viagem
        from contagem
    ),
    /*
    Filtra apenas viagens com todos os campos obrigatórios preenchidos
    */
    viagens_campos_obrigatorios as (
        select * from indice where indicador_campos_obrigatorios
    ),
    /*
    Agregação dos service_ids por feed_start_date, feed_version e route_id
    */
    trips as (
        select distinct
            feed_start_date,
            feed_version,
            route_id,
            array_agg(service_id) as service_ids,
        from {{ ref("trips_gtfs") }}
        {# from `rj-smtr.gtfs.trips` #}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
        group by 1, 2, 3
    ),
    /*
    Verifica se o serviço da viagem está planejado no GTFS
    */
    servicos_planejados_gtfs as (
        select
            v.*,
            (
                select count(*)
                from unnest(v.service_ids) as service_id
                join unnest(t.service_ids) as service_id using (service_id)
            )
            > 0 as indicador_servico_planejado_gtfs
        from viagens_campos_obrigatorios v
        left join trips t using (feed_start_date, feed_version, route_id)
    ),
    /*
    Quilometragem planejada por serviço, faixa horária e sentido
    */
    servico_planejado as (
        select
            data,
            servico,
            sentido,
            extensao,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            trip_info
        from {{ ref("servico_planejado_faixa_horaria") }}
        {# from `rj-smtr`.`planejamento`.`servico_planejado_faixa_horaria` #}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where {{ incremental_filter }}
        {% endif %}
    ),
    /*
    Desaninhamento da informação do shape_id e indicador_trajeto_alternativo do array trip_info
    */
    servico_planejado_unnested as (
        select distinct
            sp.data,
            sp.servico,
            sp.sentido,
            sp.extensao,
            sp.quilometragem,
            sp.faixa_horaria_inicio,
            sp.faixa_horaria_fim,
            trip.shape_id,
            trip.indicador_trajeto_alternativo
        from servico_planejado sp, unnest(sp.trip_info) as trip
        where trip.shape_id is not null
    ),
    /*
    Verifica se o serviço está planejado na OS e calcula a velocidade média da viagem
    */
    servicos_planejados_os as (
        select
            spg.*,
            spu.extensao as distancia_planejada,
            spu.indicador_trajeto_alternativo,
            -- fmt: off
            safe_divide(spu.extensao*3600, datetime_diff(datetime_chegada, datetime_partida, second)) as velocidade_media,
            -- fmt: on
            case
                when spu.quilometragem is not null and spu.quilometragem > 0
                then true
                when
                    (spu.quilometragem is not null and spu.quilometragem <= 0)
                    or (spu.quilometragem is null and spg.modo = "Ônibus SPPO")
                then false
            end as indicador_servico_planejado_os
        from servicos_planejados_gtfs spg
        left join
            servico_planejado_unnested spu
            on spu.servico = spg.servico
            and spu.data = spg.data
            and spu.shape_id = spg.shape_id
            and spg.datetime_partida
            between spu.faixa_horaria_inicio and spu.faixa_horaria_fim
    ),
    /*
    Verifica se a velocidade média da viagem está acima do limite máximo permitido
    */
    viagens_velocidade_media as (
        select
            *,
            velocidade_media
            >= {{ var("conformidade_velocidade_min") }}
            as indicador_acima_velocidade_max
        from servicos_planejados_os
    ),
    /*
    Verifica se a viagem está sobreposta a outra viagem do mesmo veículo
    */
    viagens_sobrepostas as (
        select
            v1.data,
            v1.id_viagem,
            v1.id_veiculo,
            v1.datetime_partida,
            v1.datetime_chegada,
            v1.datetime_captura_viagem,
            v2.id_viagem as id_viagem_sobreposta,
            v2.datetime_captura_viagem as datetime_captura_viagem_sobreposta,
            case
                when v2.id_viagem is not null
                then
                    case
                        when v1.datetime_captura_viagem = v2.datetime_captura_viagem
                        then true
                        when v1.datetime_captura_viagem < v2.datetime_captura_viagem
                        then true
                        else false
                    end
                else false
            end as indicador_viagem_sobreposta
        from viagens_velocidade_media v1
        left join
            viagens_velocidade_media v2
            on (
                v1.data between date_sub(v2.data, interval 1 day) and date_add(
                    v2.data, interval 1 day
                )
            )
            and v1.id_veiculo = v2.id_veiculo
            and v1.id_viagem != v2.id_viagem
            and v1.datetime_partida < v2.datetime_chegada
            and v1.datetime_chegada > v2.datetime_partida
        qualify
            row_number() over (
                partition by v1.id_viagem
                order by v2.datetime_captura_viagem desc, v2.datetime_partida
            )
            = 1
    ),
    /*
    Agregação dos indicadores de validação da viagem
    */
    viagens as (
        select
            vm.data,
            vm.id_viagem,
            vm.datetime_partida,
            vm.datetime_chegada,
            vm.modo,
            vm.id_veiculo,
            vm.trip_id,
            vm.route_id,
            vm.shape_id,
            vm.servico,
            vm.sentido,
            vm.distancia_planejada,
            vm.velocidade_media,
            vm.quantidade_segmentos_considerados,
            vm.quantidade_segmentos_validos,
            vm.quantidade_segmentos_necessarios,
            vm.indice_validacao,
            vs.indicador_viagem_sobreposta,
            -- fmt: off
            vm.quantidade_segmentos_validos >= vm.quantidade_segmentos_necessarios as indicador_trajeto_valido,
            -- fmt: on
            vm.indicador_servico_planejado_gtfs,
            vm.indicador_servico_planejado_os,
            vm.indicador_servico_divergente,
            vm.indicador_shape_invalido,
            vm.indicador_campos_obrigatorios,
            vm.indicador_trajeto_alternativo,
            vm.indicador_acima_velocidade_max,
            (
                vm.indicador_campos_obrigatorios and not vm.indicador_shape_invalido
                -- fmt: off
                and vm.quantidade_segmentos_validos >= vm.quantidade_segmentos_necessarios
                -- fmt: on
                and vm.indicador_servico_planejado_gtfs
                {% if var("tipo_materializacao") != "monitoramento" %}
                    and not vs.indicador_viagem_sobreposta
                {% endif %}
                and not vm.indicador_acima_velocidade_max
                and ifnull(vm.indicador_servico_planejado_os, true)
            ) as indicador_viagem_valida,
            vm.tipo_dia,
            vm.feed_version,
            vm.feed_start_date
        from viagens_velocidade_media vm
        left join viagens_sobrepostas vs using (id_viagem)
    ),
    -- fmt: off
    /*
    União entre viagens com indicadores de validação e viagens com campos obrigatórios incompletos
    */
    viagem_completa as (
        select *
        from viagens

        full outer union all by name

        select *, indicador_campos_obrigatorios as indicador_viagem_valida
        from indice
        where not indicador_campos_obrigatorios
    ),
    -- fmt: on
    /*
    Filtra viagens duplicadas considerando melhor índice de validação e maior distância
    */
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
    /*
    Filtra viagens duplicadas por horário de partida considerando maior distância planejada
    */
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
    /*
    Filtra viagens duplicadas por horário de chegada considerando maior distância planejada
    */
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
    quantidade_segmentos_considerados,
    quantidade_segmentos_validos,
    quantidade_segmentos_necessarios,
    indicador_viagem_sobreposta,
    indicador_trajeto_valido,
    indicador_servico_planejado_gtfs,
    indicador_servico_planejado_os,
    indicador_servico_divergente,
    indicador_shape_invalido,
    indicador_campos_obrigatorios,
    indicador_trajeto_alternativo,
    indicador_acima_velocidade_max,
    indicador_viagem_valida,
    tipo_dia,
    feed_version,
    feed_start_date,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
{% if var("tipo_materializacao") == "monitoramento" %} from filtro_chegada
{% else %} from viagem_completa
{% endif %}
where
    data between date('{{ var("date_range_start") }}') and date(
        '{{ var("date_range_end") }}'
    )
