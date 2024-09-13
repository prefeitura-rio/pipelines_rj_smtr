{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    tags=['geolocalizacao'],
    unique_key="id_transmissao_gps",
  )
}}

SELECT
  modo,
  EXTRACT(DATE FROM datetime_gps) AS data,
  EXTRACT(HOUR FROM datetime_gps) AS hora,
  datetime_gps,
  datetime_captura,
  id_operadora,
  operadora,
  id_servico_jae,
  -- s.servico,
  servico_jae,
  descricao_servico_jae,
  CASE
    WHEN modo = "VLT" THEN SUBSTRING(id_veiculo, 1, 3)
    WHEN modo = "BRT" THEN NULL
    ELSE id_veiculo
  END AS id_veiculo,
  id_validador,
  id_transmissao_gps,
  latitude,
  longitude,
  sentido,
  estado_equipamento,
  temperatura,
  versao_app,
  '{{ var("version") }}' as versao
FROM
(
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id_transmissao_gps ORDER BY datetime_captura DESC) AS rn
  FROM
    `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.gps_validador_aux`
  WHERE
    {% if is_incremental() %}
      DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
      -- AND datetime_captura > DATETIME("{{var('date_range_start')}}") AND datetime_captura <= DATETIME("{{var('date_range_end')}}")
    {% else %}
      DATE(data) >= "2024-09-13"
    {% endif %}
)
WHERE
  rn = 1
  AND modo != "Van"
