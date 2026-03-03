{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["trip_id", "feed_start_date"],
        alias="stop_times",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

with
    stop_times_raw as (
        select
            st.*,
            fi.feed_version,
            fi.feed_end_date,
            split(json_value(st.content, '$.arrival_time'), ":") as arrival_time_parts,
        from {{ source("br_rj_riodejaneiro_gtfs_staging", "stop_times") }} st
        join
            {{ ref("feed_info_gtfs") }} fi
            on st.data_versao = cast(fi.feed_start_date as string)
        {% if is_incremental() -%}
            where
                st.data_versao
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
                and fi.feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        {%- endif %}
    )

select
    feed_version,
    safe_cast(data_versao as date) as feed_start_date,
    feed_end_date,
    safe_cast(trip_id as string) trip_id,
    make_interval(
        hour => cast(arrival_time_parts[0] as integer),
        minute => cast(arrival_time_parts[1] as integer),
        second => cast(arrival_time_parts[2] as integer)
    ) as arrival_time,
    safe_cast(json_value(content, '$.departure_time') as datetime) departure_time,
    safe_cast(json_value(content, '$.stop_id') as string) stop_id,
    safe_cast(stop_sequence as int64) stop_sequence,
    safe_cast(json_value(content, '$.stop_headsign') as string) stop_headsign,
    safe_cast(json_value(content, '$.pickup_type') as string) pickup_type,
    safe_cast(json_value(content, '$.drop_off_type') as string) drop_off_type,
    safe_cast(json_value(content, '$.continuous_pickup') as string) continuous_pickup,
    safe_cast(
        json_value(content, '$.continuous_drop_off') as string
    ) continuous_drop_off,
    safe_cast(
        json_value(content, '$.shape_dist_traveled') as float64
    ) shape_dist_traveled,
    safe_cast(json_value(content, '$.timepoint') as string) timepoint,
    '{{ var("version") }}' as versao_modelo
from stop_times_raw
