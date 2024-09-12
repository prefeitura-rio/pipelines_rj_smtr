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
    -- {{ ref("shapes_gtfs") }}
    rj-smtr.gtfs.shapes
  WHERE
    feed_start_date = '2024-09-01'
    AND shape_id = "hj1m"
)
SELECT
  feed_version,
  feed_start_date,
  feed_end_date,
  shape_id,
  ST_MAKELINE(ARRAY_AGG(ponto_shape ORDER BY shape_pt_sequence)) AS shape,
  CONCAT("LINESTRING(", STRING_AGG(lon_lat, ", " ORDER BY shape_pt_sequence), ")") AS wkt_shape,
FROM
  shapes
GROUP BY
  1,
  2,
  3,
  4
