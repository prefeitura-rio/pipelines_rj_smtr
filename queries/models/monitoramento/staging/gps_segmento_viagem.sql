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
            schema="monitoramento_interno_teste",
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
    calendario as (
        select *
        from {{ calendario }}
        {# {% if is_incremental() %} #}
        where
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
    {# {% endif %} #}
    ),
    gps_viagem as (
        select
            data,
            gv.id_viagem,
            gv.shape_id,
            gv.geo_point_gps,
            gv.servico_viagem,
            gv.servico_gps,
            gv.timestamp_gps,
            c.feed_version,
            c.feed_start_date
        {% if var("tipo_materializacao") == "monitoramento" %}
            from {{ ref("registros_status_viagem_inferida") }} gv
        {% else %} from {{ ref("gps_viagem") }} gv
        {% endif %}
        join calendario c using (data)
        {# {% if is_incremental() %}  #}
        where {{ incremental_filter }}
    {# {% endif %} #}
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
        {# from {{ ref("segmento_shape") }} #}
        from `rj-smtr.planejamento.segmento_shape`
        {# {% if is_incremental() %} #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    {# {% endif %} #}
    ),
    servico_divergente as (
        select
            id_viagem,
            max(servico_viagem != servico_gps) as indicador_servico_divergente
        from gps_viagem
        group by 1
    ),
    gps_servico_segmento as (
        select g.id_viagem, g.shape_id, s.id_segmento, count(*) as quantidade_gps
        from gps_viagem g
        join
            segmento s
            on g.feed_version = s.feed_version
            and g.shape_id = s.shape_id
            and st_intersects(s.buffer, g.geo_point_gps)
        where g.servico_gps = g.servico_viagem
        group by all
    ),
    gps_segmento as (
        select id_viagem, g.shape_id, g.id_segmento, g.quantidade_gps,
        from gps_servico_segmento g
    ),
    trips as (
        select feed_start_date, feed_version, route_id, trip_id, shape_id
        {# from {{ ref("trips_gtfs") }} #}
        from `rj-smtr.gtfs.trips`
        {# {% if is_incremental() %} #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    {# {% endif %} #}
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
            v.indicador_viagem_sobreposta,
            c.service_ids,
            c.tipo_dia,
            c.feed_start_date,
            c.feed_version
        {% if var("tipo_materializacao") == "monitoramento" %}
            from {{ ref("viagem_inferida") }} v
        {% else %} from {{ ref("viagem_informada_monitoramento") }} v
        {% endif %}
        join calendario c using (data)
        {# {% if is_incremental() %}  #}
        where {{ incremental_filter }}
    {# {% endif %} #}
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
            v.shape_id,
            s.id_segmento,
            s.indicador_segmento_desconsiderado,
            v.servico,
            v.sentido,
            v.indicador_viagem_sobreposta,
            v.service_ids,
            v.tipo_dia,
            v.feed_version,
            v.feed_start_date
        from viagem v
        left join segmento s using (shape_id, feed_version, feed_start_date)
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
    v.servico,
    v.sentido,
    ifnull(g.quantidade_gps, 0) as quantidade_gps,
    v.indicador_viagem_sobreposta,
    v.indicador_segmento_desconsiderado,
    s.indicador_servico_divergente,
    v.feed_version,
    v.feed_start_date,
    v.service_ids,
    v.tipo_dia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_segmento v
left join gps_segmento g using (id_viagem, shape_id, id_segmento)
left join servico_divergente s using (id_viagem)
{# {% if not is_incremental() %} #}
where
    v.data <= date_sub(current_date("America/Sao_Paulo"), interval 2 day)
    {# {% endif %} #}

