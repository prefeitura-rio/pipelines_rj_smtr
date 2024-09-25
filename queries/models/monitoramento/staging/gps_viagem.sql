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

with
    viagem as (
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
            sentido
        from {{ ref("viagem_informada_monitoramento") }}
        where

            data = date_sub(date('{{ var("run_date") }}'), interval 1 day)
            -- filtro de teste REMOVER
            and date(datetime_partida) = date("2024-06-05")
            and date(datetime_chegada) = date("2024-06-05")
    ),
    gps as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
        {# from {{ ref("gps_sppo") }} #}
        where
            data
            between date_sub(date('{{ var("run_date") }}'), interval 2 day) and date(
                '{{ var("run_date") }}'
            )

    )
select
    g.data,
    g.timestamp_gps,
    g.id_veiculo,
    g.servico,
    v.sentido,
    g.latitude,
    g.longitude,
    st_geogpoint(g.longitude, g.latitude) as geo_point_gps,
    v.id_viagem,
    v.datetime_partida,
    v.datetime_chegada,
    v.trip_id,
    v.route_id,
    v.shape_id,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from gps g
join
    viagem v
    on g.timestamp_gps
    between datetime_sub(v.datetime_partida, interval 10 minute) and datetime_add(
        v.datetime_chegada, interval 10 minute
    )
    and g.id_veiculo = v.id_veiculo
    and g.servico = v.servico
