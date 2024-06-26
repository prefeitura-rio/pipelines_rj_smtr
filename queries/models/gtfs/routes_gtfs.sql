{{config(
    partition_by = { 'field' :'feed_start_date',
    'data_type' :'date',
    'granularity': 'day' },
    unique_key = ['route_id', 'feed_start_date'],
    alias = 'routes'
)}}

{% if execute and is_incremental() %}
  {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

SELECT
  fi.feed_version,
  SAFE_CAST(r.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(r.route_id AS STRING) route_id,
  SAFE_CAST(JSON_VALUE(r.content, '$.agency_id') AS STRING) agency_id,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_short_name') AS STRING) route_short_name,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_long_name') AS STRING) route_long_name,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_desc') AS STRING) route_desc,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_type') AS STRING) route_type,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_url') AS STRING) route_url,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_color') AS STRING) route_color,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_text_color') AS STRING) route_text_color,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_sort_order') AS INT64) route_sort_order,
  SAFE_CAST(JSON_VALUE(r.content, '$.continuous_pickup') AS STRING) continuous_pickup,
  SAFE_CAST(JSON_VALUE(r.content, '$.continuous_drop_off') AS STRING) continuous_drop_off,
  SAFE_CAST(JSON_VALUE(r.content, '$.network_id') AS STRING) network_id,
  '{{ var("version") }}' AS versao_modelo
 FROM
  {{ source(
    'br_rj_riodejaneiro_gtfs_staging',
    'routes'
  ) }} r
JOIN
  {{ ref('feed_info_gtfs') }} fi
ON
  r.data_versao = CAST(fi.feed_start_date AS STRING)
{% if is_incremental() -%}
  WHERE
    r.data_versao IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
    AND fi.feed_start_date IN ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}