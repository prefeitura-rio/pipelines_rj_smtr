{{
    config(
        materialized="view",
    )
}}

WITH
  validador_max AS (
    SELECT data, operadora, id_validador, count(*) as quantidade_gps
    FROM {{ ref("gps_validador") }}
    WHERE data BETWEEN "2025-11-16" AND "2025-11-30"
    GROUP BY ALL
  ),
    validador AS (
    SELECT data, operadora, id_validador
    FROM validador_max
    QUALIFY
      ROW_NUMBER()
        OVER (PARTITION BY data, operadora, id_validador ORDER BY quantidade_gps DESC)
      = 1
    ),
    viagem as (
        select
            data,
            servico,
            safe_cast(
                json_value(validador, '$.id_validador') as string
            ) as id_validador,
            extract(hour from datetime_partida) as hora,
            id_viagem,
            datetime_partida,
            id_veiculo,
            ano_fabricacao,
            coalesce(
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_regularidade_temperatura.valor'
                    ) as bool
                ),
                true
            ) as indicador_regularidade_temperatura,
            safe_cast(
                json_value(
                    indicadores, '$.percentual_temperatura_regular.valor'
                ) as numeric
            ) as percentual_temperatura_regular,
            safe_cast(
                json_value(indicadores, '$.quantidade_pos_tratamento.valor') as numeric
            ) as quantidade_pos_tratamento,
            coalesce(
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_temperatura_regular_viagem.valor'
                    ) as bool
                ),
                true
            ) as indicador_temperatura_regular_viagem,
            coalesce(
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_temperatura_zero_viagem.valor'
                    ) as bool
                ),
                true
            ) as indicador_temperatura_zero_viagem,
            coalesce(
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_temperatura_transmitida_viagem.valor'
                    ) as bool
                ),
                true
            ) as indicador_temperatura_transmitida_viagem
        from {{ ref("aux_viagem_temperatura") }}
        left join
            unnest(
                json_query_array(indicadores, '$.indicador_validador.valores')
            ) as validador
        where
            (
                ano_fabricacao <= 2019
                or data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
            )
            and safe_cast(
                json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
            )
            is true
        qualify
            row_number() over (
                partition by data, id_viagem
                order by
                    indicador_regularidade_temperatura desc,
                    percentual_temperatura_regular desc,
                    quantidade_pos_tratamento desc,
                    indicador_temperatura_transmitida_viagem desc,
                    safe_cast(json_value(validador, '$.id_validador') as string) asc
            )
            = 1
    ),
    veiculo as (
        select
            data,
            id_veiculo,
            ano_fabricacao,
            indicadores.indicador_ar_condicionado.valor as indicador_ar_condicionado,
            indicadores.indicador_falha_recorrente.valor as indicador_falha_recorrente
        from {{ ref("veiculo_regularidade_temperatura_dia") }}
        where indicadores.indicador_ar_condicionado.valor is true
    ),
    viagem_planejada as (
        select servico, data, any_value(consorcio) as consorcio
        from {{ ref("viagem_planejada") }}
        group by servico, data
    )
select
    vp.consorcio,
    vl.operadora,
    v.data,
    v.servico,
    v.id_validador,
    v.hora,
    v.id_viagem,
    v.datetime_partida,
    v.ano_fabricacao,
    v.id_veiculo,
    case
        when
            v.indicador_temperatura_transmitida_viagem is true
            and v.indicador_temperatura_zero_viagem is false
            and v.indicador_temperatura_regular_viagem is true
            and coalesce(ve.indicador_falha_recorrente, false) is false
        then true
        else false
    end as regularidade_temperatura
from viagem v
left join veiculo ve using (data, id_veiculo)
left join viagem_planejada vp using (servico, data)
left join validador vl using (data, id_validador)
