{{
  config(
    partition_by = {
      "field": "feed_start_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH ordem_servico_trajeto_alternativo AS (
  SELECT
    fi.feed_version,
    SAFE_CAST(o.data_versao AS DATE) feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(tipo_os AS STRING) tipo_os,
    SAFE_CAST(evento AS STRING) evento,
    SAFE_CAST(o.servico AS STRING) servico,
    SAFE_CAST(JSON_VALUE(o.content, "$.ativacao") AS STRING) ativacao,
    SAFE_CAST(JSON_VALUE(o.content, "$.consorcio") AS STRING) consorcio,
    SAFE_CAST(JSON_VALUE(o.content, "$.sentido") AS STRING) sentido,
    SAFE_CAST(JSON_VALUE(o.content, "$.extensao") AS FLOAT64) extensao,
    SAFE_CAST(JSON_VALUE(o.content, "$.evento") AS STRING) evento,
    SAFE_CAST(JSON_VALUE(o.content, "$.vista") AS STRING) vista,
  FROM
    {{ source("br_rj_riodejaneiro_gtfs_staging", "ordem_servico_trajeto_alternativo_sentido") }} O
  LEFT JOIN
    {{ ref("feed_info_gtfs") }} fi
  ON
    o.data_versao = CAST(fi.feed_start_date AS STRING)
  {% if is_incremental() -%}
    WHERE
      o.data_versao = "{{ var('data_versao_gtfs') }}"
      AND fi.feed_start_date = "{{ var('data_versao_gtfs') }}"
  {%- endif %}
)

SELECT
  feed_version,
  feed_start_date,
  feed_end_date,
  tipo_os,
  servico,
  consorcio,
  sentido
  vista,
  ativacao,
  CASE
    WHEN evento LIKE '[%]' THEN LOWER(evento)
    ELSE REGEXP_REPLACE(LOWER(evento), r"([a-záéíóúñüç]+)", r"[\1]")
  END AS evento,
  extensao/1000 AS extensao,
  '{{ var("version") }}' AS versao_modelo
FROM
  ordem_servico_trajeto_alternativo