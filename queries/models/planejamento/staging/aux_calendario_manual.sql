{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        materialized="incremental",
        incremental_strategy="insert_overwrite",
    )
}}

with
    datas as (
        select
            data,
            date(null) as feed_start_date,
            cast(null as string) as tipo_dia,
            case
                when data between date(2024, 09, 14) and date(2024, 09, 15)
                then "Verão + Rock in Rio"
                when data between date(2024, 09, 19) and date(2024, 09, 22)
                then "Rock in Rio"
                when data = date(2024, 10, 06)
                then "Eleição"
            end as tipo_os
        from
            unnest(
                generate_date_array(
                    {% if is_incremental() %}
                        date("{{ var('date_range_start') }}"),
                        date("{{ var('date_range_end') }}")
                    {% else %}date("2024-09-01"), current_date("America/Sao_Paulo")
                    {% endif %}
                )
            ) as data
    )
select *
from datas
where feed_start_date is not null or tipo_dia is not null or tipo_os is not null
