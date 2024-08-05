{{
  config(
    alias='linha_consorcio',
  )
}}

WITH linha_consorcio AS (
  SELECT
    data,
    SAFE_CAST(CD_CONSORCIO AS STRING) AS cd_consorcio,
    SAFE_CAST(CD_LINHA AS STRING) AS cd_linha,
    timestamp_captura,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS dt_inclusao,
    PARSE_DATE("%Y-%m-%d", SAFE_CAST(JSON_VALUE(content, '$.DT_INICIO_VALIDADE') AS STRING)) AS dt_inicio_validade,
    PARSE_DATE("%Y-%m-%d", SAFE_CAST(JSON_VALUE(content, '$.DT_FIM_VALIDADE') AS STRING)) AS dt_fim_validade
  FROM
    {{ source("br_rj_riodejaneiro_bilhetagem_staging", "linha_consorcio") }}
),
linha_consorcio_rn AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cd_consorcio, cd_linha ORDER BY timestamp_captura DESC) AS rn
  FROM
    linha_consorcio
)
SELECT
  * EXCEPT(rn)
FROM
  linha_consorcio_rn
WHERE
  rn = 1