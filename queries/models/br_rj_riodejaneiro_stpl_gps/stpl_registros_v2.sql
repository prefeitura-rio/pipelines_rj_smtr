
{{ config(
        materialized='view',
        alias='registros_v2'
  )
}}

SELECT
    SAFE_CAST(codigo AS STRING) id_veiculo,
    SAFE_CAST(DATETIME(TIMESTAMP(dataHora), "America/Sao_Paulo") AS DATETIME) timestamp_gps,
    SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) timestamp_captura,
    REPLACE(content,"None","") content,
    SAFE_CAST(data as date) data,
    SAFE_CAST(hora as int64) hora
FROM {{var('stpl_registros_staging')}}