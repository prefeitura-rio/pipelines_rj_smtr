{{
  config(
    alias='endereco',
  )
}}

WITH
    endereco AS (
        SELECT
            data,
            SAFE_CAST(NR_SEQ_ENDERECO AS STRING) AS nr_seq_endereco,
            timestamp_captura,
            SAFE_CAST(JSON_VALUE(content, '$.CD_CLIENTE') AS STRING) AS cd_cliente,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_ENDERECO') AS STRING) AS cd_tipo_endereco,
            SAFE_CAST(JSON_VALUE(content, '$.CD_TIPO_LOGRADOURO') AS STRING) AS cd_tipo_logradouro,
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', SAFE_CAST(JSON_VALUE(content, '$.DT_INCLUSAO') AS STRING)), "America/Sao_Paulo") AS dt_inclusao,
            SAFE_CAST(JSON_VALUE(content, '$.NM_BAIRRO') AS STRING) AS nm_bairro,
            SAFE_CAST(JSON_VALUE(content, '$.NM_CIDADE') AS STRING) AS nm_cidade,
            SAFE_CAST(JSON_VALUE(content, '$.NR_CEP') AS STRING) AS nr_cep,
            SAFE_CAST(JSON_VALUE(content, '$.NR_LOGRADOURO') AS STRING) AS nr_logradouro,
            SAFE_CAST(JSON_VALUE(content, '$.SG_UF') AS STRING) AS sg_uf,
            SAFE_CAST(JSON_VALUE(content, '$.TX_COMPLEMENTO_LOGRADOURO') AS STRING) AS tx_complemento_logradouro,
            SAFE_CAST(JSON_VALUE(content, '$.TX_LOGRADOURO') AS STRING) AS tx_logradouro
        FROM
            {{ source("source_jae", "endereco") }}
    ),
    endereco_rn AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nr_seq_endereco ORDER BY timestamp_captura DESC) AS rn
        FROM
            endereco
    )
SELECT
  * EXCEPT(rn)
FROM
  endereco_rn
WHERE
  rn = 1