{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite",
    unique_key="id_transacao",
  )
}}

{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endset %}

WITH staging_transacao AS (
  SELECT
    *
  FROM
    `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.transacao_riocard`
  WHERE
    {% if is_incremental() %}
      {{ incremental_filter }}
    {% else %}
      DATE(data) >= "2024-09-13"
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
    `rj-smtr.cadastro.operadoras` do
  ON
    t.cd_operadora = do.id_operadora_jae
  LEFT JOIN
    `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha` l
  ON
    t.cd_linha = l.cd_linha
  LEFT JOIN
    `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha_consorcio` lc
  ON
    t.cd_linha = lc.cd_linha
    AND (
      t.data_transacao BETWEEN lc.dt_inicio_validade AND lc.dt_fim_validade
      OR lc.dt_fim_validade IS NULL
    )
  LEFT JOIN
    `rj-smtr.cadastro.consorcios` dc
  ON
    lc.cd_consorcio = dc.id_consorcio_jae
),
transacao_deduplicada AS (
  SELECT
    * EXCEPT(rn)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_transacao ORDER BY datetime_captura DESC) AS rn
    FROM
      novos_dados
  )
  WHERE
    rn = 1
)
SELECT
  *,
  '{{ var("version") }}' AS versao
FROM
  transacao_deduplicada