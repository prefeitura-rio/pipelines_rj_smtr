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
            sentido,
            fonte_gps
        {# from {{ ref("viagem_informada_monitoramento") }} #}
        from `rj-smtr.monitoramento.viagem_informada`
        {% if is_incremental() %}
            where
                data between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
        {% endif %}
    ),
    gps_conecta as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
        {# from {{ ref("gps_sppo") }} #}
        where
            {% if is_incremental() %}
                data between date_sub(
                    date('{{ var("date_range_start") }}'), interval 1 day
                ) and date_sub(date('{{ var("date_range_end") }}'))
            {% else %} data >= date("2024-10-26")
            {% endif %}

    ),
    gps_zirix as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        from `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.gps_sppo`
        {# from {{ ref("gps_sppo_zirix") }} #}
        where
            {% if is_incremental() %}
                data between date_sub(
                    date('{{ var("date_range_start") }}'), interval 1 day
                ) and date_sub(date('{{ var("date_range_end") }}'))
            {% else %} data >= date("2024-10-26")
            {% endif %}
    ),
    gps_union as (
        select *, 'conecta' as fornecedor
        from gps_conecta

        union all

        select *, 'zirix' as fornecedor
        from gps_zirix
    )
select
    v.data,
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
    v.fonte_gps,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from gps_union g
join
    viagem v
    on g.timestamp_gps between v.datetime_partida and v.datetime_chegada
    and g.id_veiculo = v.id_veiculo
    and g.servico = v.servico
    and g.fornecedor = v.fonte_gps
