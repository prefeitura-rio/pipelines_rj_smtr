{{
  config(
    partition_by = {
      'field' :'feed_start_date',
      'data_type' :'date',
      'granularity': 'day'
    },
    alias = 'shapes_geom',
    tags=['geolocalizacao']
  )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
{% if execute and is_incremental() %}
  {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

WITH shapes AS (
  SELECT
    feed_version,
    feed_start_date,
    feed_end_date,
    shape_id,
    shape_pt_sequence,
    ST_GEOGPOINT(shape_pt_lon, shape_pt_lat) AS ponto_shape,
    CONCAT(shape_pt_lon, " ", shape_pt_lat) AS lon_lat,
  FROM
    {{ ref("shapes_gtfs") }} s
    -- rj-smtr.gtfs.shapes s
  {% if is_incremental() %}
    WHERE
      feed_start_date IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
  {% endif %}
)
SELECT
  feed_version,
  feed_start_date,
  feed_end_date,
  shape_id,
  ST_MAKELINE(ARRAY_AGG(ponto_shape ORDER BY shape_pt_sequence)) AS shape,
  CONCAT("LINESTRING(", STRING_AGG(lon_lat, ", " ORDER BY shape_pt_sequence), ")") AS wkt_shape,
  '{{ var("version") }}' as versao
FROM
  shapes
GROUP BY
  1,
  2,
  3,
  4
