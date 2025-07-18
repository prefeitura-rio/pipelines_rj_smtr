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
                then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Comerciário - (Processo.Rio MTR-DES-2024/64171)
                when data = "2024-10-28"
                then "Ponto Facultativo"  -- Ponto Facultativo - Dia do Servidor Público - (Processo.Rio MTR-DES-2024/64417)
                when data between date(2024, 11, 18) and date(2024, 11, 19)
                then "Ponto Facultativo"  -- Ponto Facultativo - G20 - (Processo.Rio MTR-DES-2024/67477)
                when data = date(2025, 02, 28)
                then "Ponto Facultativo"  -- Ponto Facultativo - Sexta-feira de Carnaval - (Processo.Rio MTR-PRO-2025/03920)
                when data between date(2025, 03, 03) and date(2025, 03, 04)
                then "Domingo"  -- Carnaval - (Processo.Rio MTR-PRO-2025/03920)
                when data = date(2025, 04, 18)
                then "Domingo"  -- Feriado - Sexta Feira Santa [Portaria MGI nº 9.783/ 2024]
                when data = date(2025, 04, 22)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 55883/2025
                when data = date(2025, 05, 02)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 56034 DE 29 DE ABRIL DE 2025
                when data = date(2025, 06, 19)
                then "Domingo"  -- DECRETO RIO Nº 56189 DE 10 DE JUNHO DE 2025 / MTR-MEM-2025/01539
                when data = date(2025, 06, 20)
                then "Ponto Facultativo"  -- DECRETO RIO Nº 56189 DE 10 DE JUNHO DE 2025 / MTR-MEM-2025/01539
            end as tipo_dia,
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
                when data between date(2024, 12, 07) and date(2024, 12, 08)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2024/72800
                when data between date(2024, 12, 14) and date(2024, 12, 15)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2024/74396
                when data = date(2024, 12, 23)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/75723
                when data between date(2024, 12, 26) and date(2024, 12, 27)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/75723
                when data = date(2024, 12, 30)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/75723
                when data = date(2024, 12, 31)
                then "Vespera de Reveillon"  -- Processo.Rio MTR-DES-2024/76453
                when data = date(2025, 01, 01)
                then "Reveillon"  -- Processo.Rio MTR-DES-2024/76453
                when data between date(2025, 01, 02) and date(2025, 01, 03)
                then "Fim de ano"  -- Processo.Rio MTR-DES-2024/77046
                when data between date(2025, 01, 11) and date(2025, 01, 12)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/00831
                when data between date(2025, 01, 18) and date(2025, 01, 20)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/01760 e MTR-DES-2025/02195
                when data between date(2025, 01, 25) and date(2025, 01, 26)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/01468
                when data between date(2025, 02, 01) and date(2025, 02, 02)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-DES-2025/04515
                when data between date(2025, 02, 08) and date(2025, 02, 09)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/02376
                when data between date(2025, 02, 15) and date(2025, 02, 16)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/03046
                when data between date(2025, 02, 22) and date(2025, 02, 23)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/03740
                when data between date(2025, 03, 01) and date(2025, 03, 04)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/03920
                when data = date(2025, 03, 05)
                then "Atípico + Verão"  -- Processo.Rio MTR-PRO-2025/03920
                when data between date(2025, 03, 08) and date(2025, 03, 09)
                then "Extraordinária - Verão"  -- Processo.Rio MTR-PRO-2025/04520
                when data between date(2025, 05, 03) and date(2025, 05, 04)
                then "Dia Atípico"  -- Processo.Rio MTR-PRO-2025/04520
                when data = date(2025, 05, 24)
                then "Dia Atípico"  -- [processo]
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
