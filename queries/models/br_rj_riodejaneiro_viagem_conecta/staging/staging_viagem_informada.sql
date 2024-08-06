{{
  config(
    alias="viagem_informada"
  )
}}

SELECT
  id_viagem,
  data,
  hora,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
  DATE(
    PARSE_DATETIME(
      '%Y-%m-%d',
      SUBSTRING(
        SAFE_CAST(JSON_VALUE(content, '$.data_viagem') AS STRING),
        0,
        10
      )
    )
  ) AS data_viagem,
  DATETIME(
    PARSE_TIMESTAMP(
      '%Y-%m-%dT%H:%M:%S',
      REPLACE(
        SAFE_CAST(JSON_VALUE(content, '$.datetime_chegada') AS STRING),
        "Z",
        ""
      )
    )
  ) AS datetime_chegada,
  DATETIME(
    PARSE_TIMESTAMP(
      '%Y-%m-%dT%H:%M:%S',
      REPLACE(
        SAFE_CAST(JSON_VALUE(content, '$.datetime_partida') AS STRING),
        "Z",
        ""
      )
    )
  ) AS datetime_partida,
  DATETIME(
    PARSE_TIMESTAMP(
      '%Y-%m-%dT%H:%M:%E*S',
      REPLACE(
        SAFE_CAST(JSON_VALUE(content, '$.datetime_processamento') AS STRING),
        "Z",
        ""
      )
    )
  ) AS datetime_processamento,
  SAFE_CAST(JSON_VALUE(content, '$.id_veiculo') AS STRING) AS id_veiculo,
  SAFE_CAST(JSON_VALUE(content, '$.route_id') AS STRING) AS route_id,
  SAFE_CAST(JSON_VALUE(content, '$.sentido') AS STRING) AS sentido,
  SAFE_CAST(JSON_VALUE(content, '$.servico') AS STRING) AS servico,
  SAFE_CAST(JSON_VALUE(content, '$.shape_id') AS STRING) AS shape_id,
  SAFE_CAST(JSON_VALUE(content, '$.trip_id') AS STRING) AS trip_id
FROM
  {{ source("br_rj_riodejaneiro_viagem_conecta_staging_dev", "viagem_informada") }}