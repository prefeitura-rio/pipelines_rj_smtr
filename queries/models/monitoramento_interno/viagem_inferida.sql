{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    aux_status as (
        select
            *,
            case
                when status_viagem = 'end'
                then
                    last_value(
                        case when status_viagem = 'start' then timestamp_gps end
                    ) over (
                        partition by id_veiculo, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and 1 preceding
                    )
            end as datetime_partida
        from {{ ref("aux_monitoramento_registros_status_trajeto") }}
        where status_viagem in ('start', 'end')
    )
select distinct
    concat(
        id_veiculo,
        "-",
        servico_viagem,
        "-",
        sentido,
        "-",
        shape_id_planejado,
        "-",
        format_datetime("%Y%m%d%H%M%S", datetime_partida)
    ) as id_viagem,
    data,
    id_empresa,
    id_veiculo,
    servico_gps,
    servico_viagem,
    trip_id,
    shape_id,
    sentido,
    distancia_planejada,
    datetime_partida,
    timestamp_gps as datetime_chegada,
    '{{ var("version") }}' as versao_modelo
from aux_status
where
    status_viagem = 'end'
    and datetime_partida is not null
    and datetime_partida < timestamp_gps
qualify
    row_number() over (
        partition by id_veiculo, shape_id, datetime_partida order by timestamp_gps
    )
    = 1
