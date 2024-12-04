{{ config(materialized="ephemeral") }}

with
    frequencies as (
        select
            *,
            split(start_time, ":") as start_time_parts,
            split(end_time, ":") as end_time_parts,
        {# from `rj-smtr.gtfs.frequencies` #}
        from {{ ref("frequencies_gtfs") }}
    )

select
    * except (start_time_parts, end_time_parts, start_time, end_time),
    div(cast(start_time_parts[0] as integer), 24) days_to_add_start,
    div(cast(end_time_parts[0] as integer), 24) days_to_add_end,
    concat(
        lpad(
            cast(
                if(
                    cast(start_time_parts[0] as integer) >= 24,
                    cast(start_time_parts[0] as integer) - 24,
                    cast(start_time_parts[0] as integer)
                ) as string
            ),
            2,
            '0'
        ),
        ":",
        start_time_parts[1],
        ":",
        start_time_parts[2]
    ) as start_time,
    concat(
        lpad(
            cast(
                if(
                    cast(end_time_parts[0] as integer) >= 24,
                    cast(end_time_parts[0] as integer) - 24,
                    cast(end_time_parts[0] as integer)
                ) as string
            ),
            2,
            '0'
        ),
        ":",
        end_time_parts[1],
        ":",
        end_time_parts[2]
    ) as end_time
from frequencies
