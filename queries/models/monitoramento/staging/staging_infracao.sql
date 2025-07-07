
{{ config(
        materialized='view',
        alias='infracao'
  )
}}


SELECT
  data,
  SAFE_CAST(JSON_VALUE(content,'$.id_infracao') AS STRING) id_infracao,
  SAFE_CAST(JSON_VALUE(content,'$.modo') AS STRING) modo,
  SAFE_CAST(JSON_VALUE(content,'$.servico') AS STRING) servico,
  SAFE_CAST(JSON_VALUE(content,'$.permissao') AS STRING) permissao,
  SAFE_CAST(JSON_VALUE(content,'$.placa') AS STRING) placa,
  SAFE_CAST(id_auto_infracao AS STRING) id_auto_infracao,
  PARSE_DATE("%d/%m/%Y", split(SAFE_CAST(JSON_VALUE(content,'$.data_infracao') AS STRING), " ")[OFFSET(0)]) data_infracao,
  PARSE_DATETIME('%d/%m/%Y %H:%M:%S',
    IF(
      REGEXP_CONTAINS(JSON_VALUE(content, '$.data_infracao'), r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}'),
      CONCAT(SAFE_CAST(JSON_VALUE(content, '$.data_infracao') AS STRING), ':00'),
      CONCAT(SAFE_CAST(JSON_VALUE(content, '$.data_infracao') AS STRING), ' 00:00:00')
    )
  ) AS datetime_infracao,
  SAFE_CAST(JSON_VALUE(content,'$.infracao') AS STRING) infracao,
  SAFE_CAST(JSON_VALUE(content,'$.valor') AS FLOAT64) valor,
  SAFE_CAST(JSON_VALUE(content,'$.status') AS STRING) status,
  IF(JSON_VALUE(content,'$.data_pagamento') = "", NULL, PARSE_DATE("%d/%m/%Y", JSON_VALUE(content,'$.data_pagamento'))) data_pagamento,
  SAFE_CAST(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_captura), SECOND), "America/Sao_Paulo" ) AS DATETIME) timestamp_captura
FROM
  {{ source('veiculo_staging','infracao') }} as t

