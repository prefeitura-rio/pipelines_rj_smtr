{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set parametro_validacao = 0.9 %}

with
    contagem as (
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
            sentido,
            count(*) as quantidade_segmentos_verificados,
            countif(quantidade_gps > 0) as quantidade_segmentos_validos,
            feed_version,
            feed_start_date,
            feed_end_date
        from {{ ref("gps_segmento_viagem") }}
        where
            data = date_sub(date('{{ var("run_date") }}'), interval 1 day)
            and not indicador_segmento_desconsiderado
        group by
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            feed_version,
            feed_start_date,
            feed_end_date
    ),
    indice as (
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
            sentido,
            quantidade_segmentos_verificados,
            quantidade_segmentos_validos,
            quantidade_segmentos_validos
            / quantidade_segmentos_verificados as indice_validacao,
            feed_version,
            feed_start_date,
            feed_end_date
        from contagem
    )
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
    sentido,
    quantidade_segmentos_verificados,
    quantidade_segmentos_validos,
    indice_validacao,
    indice_validacao >= {{ parametro_validacao }} as indicador_viagem_valida,
    {{ parametro_validacao }} as parametro_validacao,
    feed_version,
    feed_start_date,
    feed_end_date
from indice
