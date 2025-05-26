
{{ config(
        materialized='view',
        alias='registros_desaninhada'
  )
}}

SELECT
  data,
  hora,
  id_veiculo,
  timestamp_captura,
  timestamp_gps,
  SAFE_CAST(json_value(content,"$.latitude") AS FLOAT64) latitude,
  SAFE_CAST(json_value(content,"$.longitude") AS FLOAT64) longitude,
  json_value(content,"$.linha") linha,
  SAFE_CAST(json_value(content,'$.velocidade') as FLOAT64) as velocidade,
  json_value(content,'$.id_migracao_trajeto') as id_migracao_trajeto,
  json_value(content,'$.sentido') as sentido,
  json_value(content,'$.trajeto') as trajeto,
  json_value(content,'$.hodometro') as hodometro
FROM {{ ref('stpl_registros_v2') }}