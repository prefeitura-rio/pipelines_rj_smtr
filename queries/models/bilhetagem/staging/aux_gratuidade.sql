{{
  config(
    materialized="table",
    incremental_strategy="merge",
    unique_key="id_gratuidade",
    partition_by={
      "field": "id_cliente",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 10000
      }
    },
  )
}}


{% set staging_gratuidade = ref('staging_gratuidade') %}


WITH gratuidade_complete_partitions AS (
    SELECT
        CAST(CAST(cd_cliente AS FLOAT64) AS INT64) AS id_cliente,
        id AS id_gratuidade,
        tipo_gratuidade,
        deficiencia_permanente,
        rede_ensino,
        data_inclusao AS data_inicio_validade,
        timestamp_captura
    FROM
        {{ staging_gratuidade }}
),
gratuidade_deduplicada AS (
    SELECT
        * EXCEPT(rn)
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_gratuidade ORDER BY timestamp_captura DESC) AS rn
            FROM
                gratuidade_complete_partitions
        )
    WHERE
        rn = 1
)
SELECT
    id_cliente,
    id_gratuidade,
    tipo_gratuidade,
    deficiencia_permanente,
    rede_ensino,
    data_inicio_validade,
    LEAD(data_inicio_validade) OVER (PARTITION BY id_cliente ORDER BY data_inicio_validade) AS data_fim_validade,
    timestamp_captura
FROM
    gratuidade_deduplicada