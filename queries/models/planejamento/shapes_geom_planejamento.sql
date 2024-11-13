{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="shapes_geom",
        tags=["geolocalizacao"],
    )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

with
    shapes as (
        select
            feed_version,
            feed_start_date,
            feed_end_date,
            shape_id,
            shape_pt_sequence,
            st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
            concat(shape_pt_lon, " ", shape_pt_lat) as lon_lat,
        from {{ ref("shapes_gtfs") }} s
        {# `rj-smtr.gtfs.shapes` s #}
        {% if is_incremental() %}
            where
                feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        {% endif %}
    )
select
    feed_start_date,
    feed_end_date,
    feed_version,
    shape_id,
    st_makeline(array_agg(ponto_shape order by shape_pt_sequence)) as shape,
    concat(
        "LINESTRING(", string_agg(lon_lat, ", " order by shape_pt_sequence), ")"
    ) as wkt_shape,
    '{{ var("version") }}' as versao
from shapes
group by 1, 2, 3, 4
