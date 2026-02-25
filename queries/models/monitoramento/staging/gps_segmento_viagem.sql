{% if var("tipo_materializacao") == "monitoramento" %}
    {{
        config(
            materialized="incremental",
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
            incremental_strategy="insert_overwrite",
            schema="monitoramento_interno",
        )
    }}
{% else %}
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
{% endif %}

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
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
    Transações Jaé para contagem de passageiros por segmento
    */
    transacao as (
        select id_veiculo, datetime_transacao
        from {{ ref("transacao") }}
        where
            data between date("{{ var('date_range_start') }}") and date_add(
                date("{{ var('date_range_end') }}"), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
    ),
    /*
    Transações RioCard para contagem de passageiros por segmento
    */
    transacao_riocard as (
        select id_veiculo, datetime_transacao
        from {{ ref("transacao_riocard") }}
        where
            data between date("{{ var('date_range_start') }}") and date_add(
                date("{{ var('date_range_end') }}"), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
    ),
    /*
    União de transações Jaé e RioCard
    */
    transacao_unificada as (
        select *
        from transacao

        union all

        select *
        from transacao_riocard
    ),
    /*
    Dados do calendário com informações sobre feeds do GTFS, tipos de dia e service_ids
    */
    calendario as (
        select *
        from {{ calendario }}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where {{ incremental_filter }}
        {% endif %}
    ),
    /*
    Relacionamento entre dados do GPS das viagens e feed do GTFS
    */
    gps_viagem as (
        select
            data,
            gv.id_viagem,
            gv.shape_id,
            gv.geo_point_gps,
            gv.servico_viagem,
            gv.servico_gps,
            gv.datetime_gps,
            c.feed_version,
            c.feed_start_date
        {% if var("tipo_materializacao") == "monitoramento" %}
            from {{ ref("registros_status_viagem_inferida") }} gv
        {% else %} from {{ ref("gps_viagem") }} gv
        {% endif %}
        join calendario c using (data)
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where {{ incremental_filter }}
        {% endif %}
    ),
    /*
    Dados dos segmentos dos shapes
    */
    segmento as (
        select
            feed_version,
            feed_start_date,
            feed_end_date,
            shape_id,
            id_segmento,
            buffer,
            inicio_vigencia_tunel,
            fim_vigencia_tunel,
            indicador_tunel,
            indicador_segmento_desconsiderado
        from {{ ref("segmento_shape") }}
        {# from `rj-smtr.planejamento.segmento_shape` #}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    /*
    Identificação do primeiro e último segmento de cada shape com seus respectivos IDs e buffers
    */
    segmento_primeiro_ultimo as (
        select
            feed_version,
            feed_start_date,
            shape_id,
            first_value(cast(id_segmento as int64)) over w as primeiro_segmento,
            last_value(cast(id_segmento as int64)) over w_frame as ultimo_segmento,
            first_value(buffer) over w as buffer_primeiro_segmento,
            last_value(buffer) over w_frame as buffer_ultimo_segmento
        from segmento
        qualify row_number() over w = 1
        window
            w as (
                partition by feed_version, feed_start_date, shape_id
                order by cast(id_segmento as int64) asc
            ),
            w_frame as (w rows between unbounded preceding and unbounded following)
    ),
    /*
    Geometria dos pontos inicial e final dos shapes
    */
    shape_geom as (
        select feed_version, feed_start_date, shape_id, start_pt, end_pt
        from {{ ref("shapes_geom_planejamento") }}
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    /*
    Identificação de viagens com serviço divergente entre GPS e viagem informada
    */
    servico_divergente as (
        select
            id_viagem,
            max(servico_viagem != servico_gps) as indicador_servico_divergente
        from gps_viagem
        group by 1
    ),
    /*
    Cálculo da partida e chegada automáticas com base na cerca eletrônica (500m)
    GPS fora da cerca eletrônica e dentro do buffer do primeiro segmento (partida) ou último segmento (chegada)
    Conforme Art. 3º, XIII e XVI da Resolução
    */
    partida_chegada_automatica as (
        select
            g.id_viagem,
            min(
                case
                    when
                        not st_dwithin(
                            g.geo_point_gps,
                            sh.start_pt,
                            {{ var("buffer_validacao_inicio_fim_metros") }}
                        )
                        and st_intersects(spu.buffer_primeiro_segmento, g.geo_point_gps)
                    then g.datetime_gps
                end
            ) as datetime_partida_automatica,
            max(
                case
                    when
                        not st_dwithin(
                            g.geo_point_gps,
                            sh.end_pt,
                            {{ var("buffer_validacao_inicio_fim_metros") }}
                        )
                        and st_intersects(spu.buffer_ultimo_segmento, g.geo_point_gps)
                    then g.datetime_gps
                end
            ) as datetime_chegada_automatica,
            /*
            Fallback: quando partida/chegada automática é nula,
            usa último/primeiro GPS dentro da cerca eletrônica e do buffer do segmento
            */
            max(
                case
                    when
                        st_dwithin(
                            g.geo_point_gps,
                            sh.start_pt,
                            {{ var("buffer_validacao_inicio_fim_metros") }}
                        )
                        and st_intersects(spu.buffer_primeiro_segmento, g.geo_point_gps)
                    then g.datetime_gps
                end
            ) as datetime_partida_automatica_fallback,
            min(
                case
                    when
                        st_dwithin(
                            g.geo_point_gps,
                            sh.end_pt,
                            {{ var("buffer_validacao_inicio_fim_metros") }}
                        )
                        and st_intersects(spu.buffer_ultimo_segmento, g.geo_point_gps)
                    then g.datetime_gps
                end
            ) as datetime_chegada_automatica_fallback
        from gps_viagem g
        join
            shape_geom sh
            on g.feed_version = sh.feed_version
            and g.feed_start_date = sh.feed_start_date
            and g.shape_id = sh.shape_id
        join
            segmento_primeiro_ultimo spu
            on g.feed_version = spu.feed_version
            and g.feed_start_date = spu.feed_start_date
            and g.shape_id = spu.shape_id
        where g.servico_gps = g.servico_viagem
        group by g.id_viagem
    ),
    /*
    Resolução do fallback: usa a automática primária; se nula, usa o fallback (dentro da cerca)
    */
    partida_chegada_automatica_fallback as (
        select
            id_viagem,
            coalesce(
                datetime_partida_automatica, datetime_partida_automatica_fallback
            ) as datetime_partida_automatica,
            coalesce(
                datetime_chegada_automatica, datetime_chegada_automatica_fallback
            ) as datetime_chegada_automatica
        from partida_chegada_automatica
    ),
    /*
    Relacionamento das viagens com dados do feed do GTFS, tipo de dia e service_ids,
    incluindo cálculo de partida/chegada considerada (cerca eletrônica)
    */
    viagem as (
        select
            data,
            v.id_viagem,
            {% if var("tipo_materializacao") == "monitoramento" %}
                cast(null as string) as id_viagem_planejada,
            {% else %} v.id_viagem_planejada,
            {% endif %}
            v.datetime_partida as datetime_partida_informada,
            v.datetime_chegada as datetime_chegada_informada,
            pca.datetime_partida_automatica,
            pca.datetime_chegada_automatica,
            case
                when
                    pca.datetime_partida_automatica is not null
                    and abs(
                        datetime_diff(
                            pca.datetime_partida_automatica, v.datetime_partida, minute
                        )
                    )
                    > {{ var("limite_validacao_inicio_fim_minutos") }}
                then pca.datetime_partida_automatica
                else v.datetime_partida
            end as datetime_partida_considerada,
            case
                when
                    pca.datetime_chegada_automatica is not null
                    and abs(
                        datetime_diff(
                            pca.datetime_chegada_automatica, v.datetime_chegada, minute
                        )
                    )
                    > {{ var("limite_validacao_inicio_fim_minutos") }}
                then pca.datetime_chegada_automatica
                else v.datetime_chegada
            end as datetime_chegada_considerada,
            v.modo,
            v.id_veiculo,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.servico,
            v.sentido,
            c.service_ids,
            c.tipo_dia,
            c.feed_start_date,
            c.feed_version,
        {% if var("tipo_materializacao") == "monitoramento" %}
                false as indicador_processamento_posterior_captura,
                false as indicador_processamento_anterior_chegada,
                true as indicador_prazo_envio,
                v.datetime_ultima_atualizacao as datetime_captura_viagem
            from {{ ref("viagem_inferida") }} v
        {% else %}
                v.indicador_processamento_posterior_captura,
                v.indicador_processamento_anterior_chegada,
                v.indicador_prazo_envio,
                v.datetime_captura as datetime_captura_viagem
            from {{ ref("viagem_informada_monitoramento") }} v
        {% endif %}
        join calendario c using (data)
        left join partida_chegada_automatica_fallback pca using (id_viagem)
        {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %}
            where {{ incremental_filter }}
        {% endif %}
    ),
    /*
    Ponto médio temporal de cada viagem para desambiguação
    de GPS em rotas circulares (primeiro e último segmento se sobrepõem)
    */
    midpoint_viagem as (
        select
            id_viagem,
            datetime_add(
                datetime_partida_considerada,
                interval cast(
                    datetime_diff(
                        datetime_chegada_considerada,
                        datetime_partida_considerada,
                        second
                    )
                    / 2 as int64
                ) second
            ) as datetime_midpoint
        from viagem
    ),
    /*
    Contagem de registros de GPS por segmento da viagem, aplicando regras de vigência para túneis,
    filtrando apenas GPS entre partida e chegada consideradas (Art. 5, par. 1)
    */
    gps_servico_segmento as (
        select
            g.id_viagem,
            g.shape_id,
            s.id_segmento,
            count(*) as quantidade_gps,
            min(g.datetime_gps) as datetime_primeiro_gps_segmento,
            max(g.datetime_gps) as datetime_ultimo_gps_segmento
        from gps_viagem g
        join
            segmento s
            on g.feed_version = s.feed_version
            and g.feed_start_date = s.feed_start_date
            and g.shape_id = s.shape_id
            and st_intersects(s.buffer, g.geo_point_gps)
            and (
                (
                    s.indicador_tunel
                    and (
                        (
                            g.data
                            between s.inicio_vigencia_tunel and s.fim_vigencia_tunel
                        )
                        or (
                            g.data >= s.inicio_vigencia_tunel
                            and s.fim_vigencia_tunel is null
                        )
                    )
                )
                or (s.inicio_vigencia_tunel is null and s.fim_vigencia_tunel is null)
            )
        join viagem v on g.id_viagem = v.id_viagem
        join
            segmento_primeiro_ultimo spu
            on s.feed_version = spu.feed_version
            and s.feed_start_date = spu.feed_start_date
            and s.shape_id = spu.shape_id
        join midpoint_viagem mp on g.id_viagem = mp.id_viagem
        where
            g.servico_gps = g.servico_viagem
            and g.datetime_gps
            between v.datetime_partida_considerada and v.datetime_chegada_considerada
            -- Desambiguação temporal para rotas circulares
            -- (primeiro e último segmento se sobrepõem geograficamente)
            and (
                -- Segmentos do meio: sem restrição temporal
                (
                    cast(s.id_segmento as int64) != spu.primeiro_segmento
                    and cast(s.id_segmento as int64) != spu.ultimo_segmento
                )
                -- Primeiro segmento: GPS antes do ponto médio da viagem
                or (
                    cast(s.id_segmento as int64) = spu.primeiro_segmento
                    and g.datetime_gps < mp.datetime_midpoint
                )
                -- Último segmento: GPS a partir do ponto médio da viagem
                or (
                    cast(s.id_segmento as int64) = spu.ultimo_segmento
                    and g.datetime_gps >= mp.datetime_midpoint
                )
            )
        group by all
    ),
    /*
    Relacionamento das viagens com os segmentos, aplicando regras de vigência para túneis
    */
    viagem_segmento as (
        select
            v.data,
            v.id_viagem,
            v.id_viagem_planejada,
            v.datetime_partida_informada,
            v.datetime_chegada_informada,
            v.datetime_partida_automatica,
            v.datetime_chegada_automatica,
            v.datetime_partida_considerada,
            v.datetime_chegada_considerada,
            v.modo,
            v.id_veiculo,
            v.trip_id,
            v.route_id,
            v.shape_id,
            s.id_segmento,
            s.indicador_segmento_desconsiderado,
            v.servico,
            v.sentido,
            v.service_ids,
            v.tipo_dia,
            v.feed_version,
            v.feed_start_date,
            v.indicador_processamento_posterior_captura,
            v.indicador_processamento_anterior_chegada,
            v.indicador_prazo_envio,
            v.datetime_captura_viagem
        from viagem v
        left join
            segmento s
            on v.feed_version = s.feed_version
            and v.shape_id = s.shape_id
            and v.feed_start_date = s.feed_start_date
            and (
                (
                    s.indicador_tunel
                    and (
                        (
                            v.data
                            between s.inicio_vigencia_tunel and s.fim_vigencia_tunel
                        )
                        or (
                            v.data >= s.inicio_vigencia_tunel
                            and s.fim_vigencia_tunel is null
                        )
                    )
                )
                or (s.inicio_vigencia_tunel is null and s.fim_vigencia_tunel is null)
            )
    ),
    /*
    Associação dos segmentos com dados de GPS e cálculo de datetime de início/fim por segmento
    */
    segmento_com_datetime as (
        select
            v.*,
            ifnull(g.quantidade_gps, 0) as quantidade_gps,
            g.datetime_primeiro_gps_segmento,
            g.datetime_ultimo_gps_segmento,
            case
                when
                    cast(v.id_segmento as int64)
                    = min(cast(v.id_segmento as int64)) over (partition by v.id_viagem)
                then v.datetime_partida_considerada
                else g.datetime_primeiro_gps_segmento
            end as datetime_inicio_segmento,
            case
                when
                    cast(v.id_segmento as int64)
                    = max(cast(v.id_segmento as int64)) over (partition by v.id_viagem)
                then v.datetime_chegada_considerada
                else g.datetime_ultimo_gps_segmento
            end as datetime_fim_segmento
        from viagem_segmento v
        left join gps_servico_segmento g using (id_viagem, shape_id, id_segmento)
    ),
    /*
    Contagem de passageiros por segmento com base nas transações unificadas
    */
    transacao_segmento as (
        select
            s.id_viagem,
            s.shape_id,
            s.id_segmento,
            count(t.datetime_transacao) as quantidade_passageiros
        from segmento_com_datetime s
        left join
            transacao_unificada t
            on t.id_veiculo = substr(s.id_veiculo, 2)
            and t.datetime_transacao
            between s.datetime_inicio_segmento and s.datetime_fim_segmento
        where
            s.datetime_inicio_segmento is not null
            and s.datetime_fim_segmento is not null
        group by 1, 2, 3
    )
select
    v.data,
    v.id_viagem,
    v.id_viagem_planejada,
    v.datetime_partida_informada,
    v.datetime_chegada_informada,
    v.datetime_partida_automatica,
    v.datetime_chegada_automatica,
    v.datetime_partida_considerada,
    v.datetime_chegada_considerada,
    v.modo,
    v.id_veiculo,
    v.trip_id,
    v.route_id,
    v.shape_id,
    v.id_segmento,
    v.servico,
    v.sentido,
    v.quantidade_gps,
    v.indicador_segmento_desconsiderado,
    s.indicador_servico_divergente,
    v.datetime_inicio_segmento,
    v.datetime_fim_segmento,
    ifnull(ts.quantidade_passageiros, 0) as quantidade_passageiros,
    v.feed_version,
    v.feed_start_date,
    v.service_ids,
    v.tipo_dia,
    v.indicador_processamento_posterior_captura,
    v.indicador_processamento_anterior_chegada,
    v.indicador_prazo_envio,
    v.datetime_captura_viagem,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from segmento_com_datetime v
left join transacao_segmento ts using (id_viagem, shape_id, id_segmento)
left join servico_divergente s using (id_viagem)
{% if not is_incremental() and var("tipo_materializacao") != "monitoramento" %}
    where v.data <= date_sub(current_date("America/Sao_Paulo"), interval 2 day)
{% endif %}
