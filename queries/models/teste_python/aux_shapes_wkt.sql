{{
  config(
    materialized="view"
  )

}}
with shapes AS (
  select
    feed_start_date,
    shape_id,
    shape_pt_sequence,
    CONCAT(shape_pt_lon, " ", shape_pt_lat) AS lon_lat
  from
    rj-smtr.gtfs.shapes
  where
    feed_start_date = '2024-09-01'
    and shape_id = "hj1m"
   order by
    1,
    2,
    3
)
SELECT
  feed_start_date,
  shape_id,
  concat("LINESTRING(", string_agg(lon_lat, ", "), ")") AS shape_wkt
from
  shapes
group by
  1,
  2