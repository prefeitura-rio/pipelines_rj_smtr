{{
  config(
    alias='linha',
  )
}}

WITH
    produto AS (
        SELECT
            data,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
            SAFE_CAST(CD_PRODUTO AS STRING) AS cd_produto,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS datetime_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.CD_EMISSOR_PRODUTO') AS STRING) AS cd_emissor_produto,
            SAFE_CAST(JSON_VALUE(content, '$.NM_PRODUTO') AS STRING) AS nm_produto,
            SAFE_CAST(JSON_VALUE(content, '$.TX_DESCRITIVO_PRODUTO') AS STRING) AS tx_descritivo_produto
        FROM
            {{ source("source_jae", "produto") }}
    ),
    produto_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cd_produto ORDER BY timestamp_captura DESC) AS rn
        FROM
            produto
    )
SELECT
  * EXCEPT(rn)
FROM
  produto_rn
WHERE
  rn = 1