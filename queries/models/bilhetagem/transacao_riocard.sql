{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite",
  )
}}

{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% set transacao_staging = ref('staging_transacao_riocard') %}
{% if execute and is_incremental() %}
  {% set transacao_partitions_query %}
    SELECT DISTINCT
      CONCAT("'", DATE(data_transacao), "'") AS data_transacao
    FROM
      {{ transacao_staging }}
    WHERE
      {{ incremental_filter }}
  {% endset %}

  {% set transacao_partitions = run_query(transacao_partitions_query).columns[0].values() %}
{% endif %}

WITH staging_transacao AS (
  SELECT
    *
  FROM
    {{ transacao_staging }}
  {% if is_incremental() %}
    WHERE
      {{ incremental_filter }}
  {% endif %}
),
novos_dados AS (
  SELECT
    EXTRACT(DATE FROM t.data_transacao) AS data,
    EXTRACT(HOUR FROM t.data_transacao) AS hora,
    t.data_transacao AS datetime_transacao,
    t.data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    COALESCE(do.modo, dc.modo) AS modo,
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    t.cd_linha AS id_servico_jae,
    l.nr_linha AS servico_jae,
    l.nm_linha AS descricao_servico_jae,
    t.sentido,
    CASE
      WHEN do.modo = "VLT" THEN SUBSTRING(t.veiculo_id, 1, 3)
      WHEN do.modo = "BRT" THEN NULL
      ELSE t.veiculo_id
    END AS id_veiculo,
    t.numero_serie_validador AS id_validador,
    t.id AS id_transacao,
    t.latitude_trx AS latitude,
    t.longitude_trx AS longitude,
    ST_GEOGPOINT(t.longitude_trx, t.latitude_trx) AS geo_point_transacao,
    t.valor_transacao
  FROM
    staging_transacao t
  LEFT JOIN
    {{ ref("operadoras") }} do
  ON
    t.cd_operadora = do.id_operadora_jae
  LEFT JOIN
    {{ ref("staging_linha") }} l
  ON
    t.cd_linha = l.cd_linha
  LEFT JOIN
    {{ ref("staging_linha_consorcio") }} lc
  ON
    t.cd_linha = lc.cd_linha
    AND (
      t.data_transacao BETWEEN lc.dt_inicio_validade AND lc.dt_fim_validade
      OR lc.dt_fim_validade IS NULL
    )
  LEFT JOIN
    {{ ref("consorcios") }} dc
  ON
    lc.cd_consorcio = dc.id_consorcio_jae
),
-- consorcios AS (
--   SELECT
--     t.data,
--     t.hora,
--     t.datetime_transacao,
--     t.datetime_processamento,
--     t.datetime_captura,
--     COALESCE(t.modo, dc.modo) AS modo,
--     dc.id_consorcio,
--     dc.consorcio,
--     t.id_operadora,
--     t.operadora,
--     t.id_servico_jae,
--     t.servico_jae,
--     t.descricao_servico_jae,
--     t.sentido,
--     t.id_veiculo,
--     t.id_validador,
--     t.id_transacao,
--     t.latitude,
--     t.longitude,
--     t.valor_transacao
--   FROM
--     novos_dados t
--   LEFT JOIN
--     {{ ref("consorcios") }} dc
--   USING(id_consorcio_jae)
-- ),
particoes_completas AS (
  SELECT
    *,
    0 AS priority
  FROM
    novos_dados

  {% if is_incremental() and transacao_partitions|length > 0 %}
    UNION ALL

    SELECT
      * EXCEPT(versao),
      1 AS priority
    FROM
      {{ this }}
    WHERE
      data IN ({{ transacao_partitions|join(', ') }})
  {% endif %}
),
transacao_deduplicada AS (
  SELECT
    * EXCEPT(rn, priority)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_transacao ORDER BY datetime_captura DESC, priority) AS rn
    FROM
      particoes_completas
  )
  WHERE
    rn = 1
)
SELECT
  *,
  '{{ var("version") }}' AS versao
FROM
  transacao_deduplicada