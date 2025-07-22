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
    viagem_temperatura as (
        select
            data,
            id_viagem,
            id_veiculo,
            ano_fabricacao,
            safe_cast(
                json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
            ) as indicador_ar_condicionado,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_variacao.valor'
                ) as bool
            ) as indicador_temperatura_variacao,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_transmitida.valor'
                ) as bool
            ) as indicador_temperatura_transmitida,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_descartada.valor'
                ) as bool
            ) as indicador_temperatura_descartada
        from {{ ref("aux_viagem_temperatura") }}
        where
            data
            between date_sub(date("{{var('start_date')}}"), interval 4 day) and date(
                "{{var('end_date')}}"
            )
            and data >= date("{{ var('DATA_SUBSIDIO_V16_INICIO') }}")
    ),
    agg_veiculo as (
        select
            data,
            id_veiculo,
            ano_fabricacao,
            indicador_ar_condicionado,
            max(indicador_temperatura_variacao) as indicador_temperatura_variacao,
            max(indicador_temperatura_transmitida) as indicador_temperatura_transmitida,
            max(indicador_temperatura_descartada) as indicador_temperatura_descartada
        from viagem_temperatura
        group by all
    ),
    falha_dia as (
        select
            *,
            case
                when
                    not indicador_temperatura_variacao
                    or not indicador_temperatura_transmitida
                    or (indicador_temperatura_descartada)
                then true
                else false
            end as indicio_falha
        from agg_veiculo
    ),
    dias_com_falha_consecutiva as (
        select
            *,
            countif(indicio_falha) over (
                partition by id_veiculo
                order by data
                rows between 4 preceding and current row
            ) as quantidade_dia_falha_operacional
        from falha_dia
    ),
    veiculo_com_falha as (
        select
            *,
            quantidade_dia_falha_operacional = 5 as indicador_falha,
            case
                when quantidade_dia_falha_operacional = 5
                then
                    (
                        select string_agg(motivo, ', ')
                        from
                            unnest(
                                [
                                    if(
                                        not indicador_temperatura_variacao,
                                        "Repetição de valores ao longo do dia",
                                        ""
                                    ),
                                    if(
                                        not indicador_temperatura_transmitida,
                                        "Ausência total de dados",
                                        ""
                                    ),
                                    if(
                                        indicador_temperatura_descartada,
                                        "Mais de 50% dos registros descartados",
                                        ""
                                    )
                                ]
                            ) as motivo
                        where motivo != ""
                    )
                else null
            end as motivo
        from dias_com_falha_consecutiva
    )
select
    data,
    id_veiculo,
    ano_fabricacao,
    indicador_ar_condicionado,
    indicador_temperatura_variacao,
    indicador_temperatura_transmitida,
    indicador_temperatura_descartada,
    quantidade_dia_falha_operacional,
    indicador_falha,
    motivo
from veiculo_com_falha
