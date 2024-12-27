{{
  config(
    alias='linha_consorcio_operadora_transporte',
  )
}}

WITH linha_consorcio_operadora_transporte AS (
  SELECT
    data,
    SAFE_CAST(CD_CONSORCIO AS STRING) AS cd_consorcio,
    SAFE_CAST(CD_OPERADORA_TRANSPORTE AS STRING) AS cd_operadora_transporte,
    SAFE_CAST(CD_LINHA AS STRING) AS cd_linha,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS dt_inclusao,
    PARSE_DATE("%Y-%m-%d", SAFE_CAST(JSON_VALUE(content, '$.DT_INICIO_VALIDADE') AS STRING)) AS dt_inicio_validade,
    PARSE_DATE("%Y-%m-%d", SAFE_CAST(JSON_VALUE(content, '$.DT_FIM_VALIDADE') AS STRING)) AS dt_fim_validade
  FROM
    {{ source("br_rj_riodejaneiro_bilhetagem_staging", "linha_consorcio_operadora_transporte") }}
),
linha_consorcio_operadora_transporte_rn AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cd_consorcio, cd_operadora_transporte, cd_linha ORDER BY timestamp_captura DESC) AS rn
  FROM
    linha_consorcio_operadora_transporte
)
SELECT
  * EXCEPT(rn)
FROM
  linha_consorcio_operadora_transporte_rn
WHERE
  rn = 1