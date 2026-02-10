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
    {# {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %} #}
        {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
        {% endset %}

        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {# {% endif %} #}
{% endif %}

with
    /*
    Dados do calendário com informações sobre feeds do GTFS, tipos de dia e service_ids
    */
    calendario as (
        select *
        from {{ calendario }}
        {# {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %} #}
            where {{ incremental_filter }}
        {# {% endif %} #}
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
        {# {% if var("tipo_materializacao") == "monitoramento" %} #}
            {# from {{ ref("registros_status_viagem_inferida") }} gv #}
        {# {% else %}  #}
        from {{ ref("gps_viagem") }} gv
        {# {% endif %} #}
        join calendario c using (data)
        {# {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %} #}
            where {{ incremental_filter }}
        {# {% endif %} #}
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
        {# {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %} #}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {# {% endif %} #}
    ),
    /*
    Geometria dos pontos inicial e final dos shapes
    */
    shape_geom as (
        select
            feed_version,
            feed_start_date,
            shape_id,
            start_pt,
            end_pt
        from {{ ref("shapes_geom_planejamento") }}
        {# {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %} #}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {# {% endif %} #}
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
    GPS fora da cerca eletrônica e dentro do buffer de um segmento
    */
    partida_chegada_automatica as (
        select
            g.id_viagem,
            min(
                case
                    when not st_dwithin(g.geo_point_gps, sh.start_pt, 500)
                    then g.datetime_gps
                end
            ) as datetime_partida_automatica,
            max(
                case
                    when not st_dwithin(g.geo_point_gps, sh.end_pt, 500)
                    then g.datetime_gps
                end
            ) as datetime_chegada_automatica
        from gps_viagem g
        join
            shape_geom sh
            on g.feed_version = sh.feed_version
            and g.shape_id = sh.shape_id
        join
            segmento s
            on g.feed_version = s.feed_version
            and g.shape_id = s.shape_id
            and st_intersects(s.buffer, g.geo_point_gps)
        where g.servico_gps = g.servico_viagem
        group by g.id_viagem
    ),
    /*
    Relacionamento das viagens com dados do feed do GTFS, tipo de dia e service_ids,
    incluindo cálculo de partida/chegada considerada (cerca eletrônica)
    */
    viagem as (
        select
            data,
            v.id_viagem,
            v.datetime_partida as datetime_partida_informada,
            v.datetime_chegada as datetime_chegada_informada,
            pca.datetime_partida_automatica,
            pca.datetime_chegada_automatica,
            case
                when
                    pca.datetime_partida_automatica is not null
                    and abs(
                        datetime_diff(
                            pca.datetime_partida_automatica,
                            v.datetime_partida,
                            second
                        )
                    )
                    > 600
                then pca.datetime_partida_automatica
                else v.datetime_partida
            end as datetime_partida_considerada,
            case
                when
                    pca.datetime_chegada_automatica is not null
                    and abs(
                        datetime_diff(
                            pca.datetime_chegada_automatica,
                            v.datetime_chegada,
                            second
                        )
                    )
                    > 600
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
        {# {% if var("tipo_materializacao") == "monitoramento" %}
                v.datetime_ultima_atualizacao as datetime_captura_viagem
            from {{ ref("viagem_inferida") }} v
        {% else %} #}
                v.datetime_captura as datetime_captura_viagem
            from {{ ref("viagem_informada_monitoramento") }} v
        {# {% endif %} #}
        join calendario c using (data)
        left join partida_chegada_automatica pca using (id_viagem)
        {# {% if is_incremental() or var("tipo_materializacao") == "monitoramento" %} #}
            where {{ incremental_filter }}
        {# {% endif %} #}
    ),
    /*
    Contagem de registros de GPS por segmento da viagem, aplicando regras de vigência para túneis,
    filtrando apenas GPS entre partida e chegada consideradas (Art. 5, par. 1)
    */
    gps_servico_segmento as (
        select g.id_viagem, g.shape_id, s.id_segmento, count(*) as quantidade_gps
        from gps_viagem g
        join
            segmento s
            on g.feed_version = s.feed_version
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
        join
            viagem v
            on g.id_viagem = v.id_viagem
        where
            g.servico_gps = g.servico_viagem
            and g.datetime_gps
            between v.datetime_partida_considerada and v.datetime_chegada_considerada
        group by all
    ),
    /*
    Relacionamento das viagens com os segmentos, aplicando regras de vigência para túneis
    */
    viagem_segmento as (
        select
            v.data,
            v.id_viagem,
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
    )
select
    v.data,
    id_viagem,
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
    shape_id,
    id_segmento,
    v.servico,
    v.sentido,
    ifnull(g.quantidade_gps, 0) as quantidade_gps,
    v.indicador_segmento_desconsiderado,
    s.indicador_servico_divergente,
    v.feed_version,
    v.feed_start_date,
    v.service_ids,
    v.tipo_dia,
    v.datetime_captura_viagem,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_segmento v
left join gps_servico_segmento g using (id_viagem, shape_id, id_segmento)
left join servico_divergente s using (id_viagem)
{# {% if not is_incremental() and var("tipo_materializacao") != "monitoramento" %}
    where v.data <= date_sub(current_date("America/Sao_Paulo"), interval 2 day)
{% endif %} #}
