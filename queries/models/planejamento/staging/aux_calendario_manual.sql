{{ config(materialized="ephemeral") }}

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
            date({{ var("data_versao_gtfs") }}),
            date_add(date({{ var("data_versao_gtfs") }}), interval 1 year)
        )
    ) as data
