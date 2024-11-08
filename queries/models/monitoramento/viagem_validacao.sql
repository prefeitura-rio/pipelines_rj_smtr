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


{% set parametro_validacao = 0.9 %}

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
            tipo_dia,
            feed_version,
            feed_start_date
        from {{ ref("gps_segmento_viagem") }}
        where
            not indicador_segmento_desconsiderado
            {% if is_incremental() %} {{ incremental_filter }} {% endif %}
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
            quantidade_segmentos_validos
            / quantidade_segmentos_verificados as indice_validacao,
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
            case
                when service_id like "D_%"
                then "Domingo"
                when service_id like "S_%"
                then "Sabado"
                when service_id like "U_%"
                then "Dia Útil"
            end as tipo_dia
        from {{ ref("trips_gtfs") }}
        where
            (service_id like "D_%" or service_id like "S_%" or service_id like "U_%")
            {% if is_incremental() %}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% endif %}
    ),
    servicos_planejados as (
        select i.*, t.tipo_dia is not null as indicador_servico_planejado
        from indice i
        left join trips t using (feed_start_date, feed_version, route_id, tipo_dia)
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
    indice_validacao >= {{ parametro_validacao }} as indicador_trajeto_valido,
    indicador_servico_planejado,
    indice_validacao >= {{ parametro_validacao }}
    and indicador_servico_planejado as indicador_viagem_valida,
    {{ parametro_validacao }} as parametro_validacao,
    tipo_dia,
    feed_version,
    feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from servicos_planejados
