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
            case
                when data = "2024-10-21"
                then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Comérciario - (Processo.Rio MTR-DES-2024/64171)
                when data = "2024-10-28"
                then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Servidor Público - (Processo.Rio MTR-DES-2024/64417)
                when data between date(2024, 11, 18) and date(2024, 11, 19)
                then "Ponto Facultativo"  -- Ponto Facultativo - G20 - (Processo.Rio MTR-DES-2024/67477)
            end as tipo_dia
            case
                when data between date(2024, 09, 14) and date(2024, 09, 15)
                then "Verão + Rock in Rio"
                when data between date(2024, 09, 19) and date(2024, 09, 22)
                then "Rock in Rio"
                when data = date(2024, 10, 06)
                then "Eleição"
                when data = date(2024, 11, 03)
                then "Enem"
                when data = date(2024, 11, 10)
                then "Enem"
                when data = date(2024, 11, 24)
                then "Parada LGBTQI+"  -- Processo.Rio MTR-DES-2024/70057
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
