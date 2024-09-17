{{
  config(
    partition_by = {
      'field' :'feed_start_date',
      'data_type' :'date',
      'granularity': 'day'
    },
    tags=['geolocalizacao']
  )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
{% if execute and is_incremental() %}
  {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

WITH aux_segmento AS (
  SELECT
    feed_version,
    feed_start_date,
    feed_end_date,
    shape_id,
    id_segmento,
    ST_GEOGFROMTEXT(wkt_segmento) AS segmento,
    wkt_segmento,
    ROUND(ST_LENGTH(ST_GEOGFROMTEXT(wkt_segmento)), 1) AS comprimento_segmento
  FROM
    {{ ref("aux_segmento_shape") }}
),
tunel AS (
  SELECT
    ST_UNION_AGG(ST_BUFFER(geometry, 50)) AS buffer_tunel
  FROM
    {{ source("dados_mestres", "logradouro") }}
  WHERE
    tipo = "Túnel"

),
buffer_segmento AS (
  SELECT
    *,
    ST_BUFFER(segmento, 20) AS buffer_completo,
  FROM
    aux_segmento
),
intercessao_segmento AS (
  SELECT
    b1.shape_id,
    b1.id_segmento,
    ST_UNION(ARRAY_AGG(b2.buffer_completo) )AS buffer_segmento_posterior
  FROM
    buffer_segmento b1
  JOIN
    buffer_segmento b2
  ON
    b1.shape_id = b2.shape_id
    AND b1.id_segmento < b2.id_segmento
    AND ST_INTERSECTS(b1.buffer_completo, b2.buffer_completo)
  GROUP BY
    1,
    2
),
buffer_segmento_recortado AS (
  SELECT
    b.*,
    COALESCE(
      ST_DIFFERENCE(
        buffer_completo,
        i.buffer_segmento_posterior
      ),
      buffer_completo
    ) AS buffer
  FROM
    buffer_segmento b
  LEFT JOIN
    intercessao_segmento i
  USING(shape_id, id_segmento)
),
indicador_validacao_shape AS (
  SELECT
    s.*,
    ST_INTERSECTS(s.segmento, t.buffer_tunel) AS indicador_tunel,
    ST_AREA(s.buffer) / ST_AREA(s.buffer_completo) < 0.5 AS indicador_area_prejudicada,
    s.comprimento_segmento < 990 AS indicador_segmento_pequeno
  FROM
    buffer_segmento_recortado s
  CROSS JOIN
    tunel t
)
SELECT
  *,
  (
    (
      indicador_tunel
      AND (
        (id_segmento > 1)
        OR (shape_id < MAX(id_segmento) OVER (PARTITION BY feed_start_date, shape_id))
      )
    )
    OR indicador_area_prejudicada
    OR indicador_segmento_pequeno
  ) AS indicador_segmento_desconsiderado,
  '{{ var("version") }}' AS versao
FROM
  indicador_validacao_shape

{% if is_incremental() %}

  UNION ALL

  SELECT
    s.feed_version
    s.feed_start_date
    fi.feed_end_date
    s.shape_id
    s.id_segmento
    s.segmento
    s.wkt_segmento
    s.comprimento_segmento,
    s.buffer_completo,
    s.buffer,
    s.indicador_tunel,
    s.indicador_area_prejudicada,
    s.indicador_segmento_pequeno,
    s.indicador_segmento_desconsiderado,
    s.versao
  FROM
    {{ this }} s
  JOIN
    {{ ref('feed_info_gtfs') }} fi
  USING(feed_start_date)
  WHERE
    feed_start_date = '{{ last_feed_version }}'
{% endif %}
