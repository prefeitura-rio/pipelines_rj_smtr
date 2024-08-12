
{{ config(
        materialized='view',
        alias='registros'
  )
}}

SELECT
    SAFE_CAST(codigo AS STRING) codigo,
    SAFE_CAST(placa AS STRING) placa,
    SAFE_CAST(linha AS STRING) linha,
    SAFE_CAST(latitude AS FLOAT64) latitude,
    SAFE_CAST(longitude AS FLOAT64) longitude,
    SAFE_CAST(DATETIME(TIMESTAMP(dataHora), "America/Sao_Paulo") AS DATETIME) timestamp_gps,
    SAFE_CAST(velocidade AS INT64) velocidade,
    SAFE_CAST(id_migracao_trajeto AS STRING) id_migracao_trajeto,
    SAFE_CAST(sentido AS STRING) sentido,
    SAFE_CAST(trajeto AS STRING) trajeto,
    SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(hora AS INT64) hora
from {{var('stpl_registros_staging')}}  as t