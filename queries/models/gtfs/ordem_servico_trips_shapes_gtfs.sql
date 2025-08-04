{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="ordem_servico_trips_shapes",
    )
}}
{% if var("data_versao_gtfs") < var("DATA_GTFS_V4_INICIO") %}
select *
from {{ ref("ordem_servico_trips_shapes_gtfs_v1") }}
where
    feed_start_date < date("{{ var('DATA_GTFS_V4_INICIO') }}")
union all by name
{% endif %}
select *
from {{ ref("ordem_servico_trips_shapes_gtfs_v2") }}
where
    feed_start_date >= date("{{ var('DATA_GTFS_V4_INICIO') }}")
