{{
    config(
        materialized="table",
    )
}}
with

    -- 1. CTE para paradas individuais (location_type = 0 ou nulo)
    stops_base as (
        select
            stop_id,
            stop_code,
            stop_name,
            stop_lat,
            stop_lon,
            location_type,
            parent_station,
            wheelchair_boarding,
            st_geogpoint(stop_lon, stop_lat) as stop_geography
        from {{ ref("stops_gtfs") }}
        where
            -- Opcional: filtrar por uma data específica se necessário
            feed_start_date = "2025-07-16"
            and (location_type = "0" or location_type is null)
            and stop_lat is not null
            and stop_lon is not null
    ),

    -- 2. CTE para obter os dados das estações-mãe (location_type = 1)
    stations as (
        select
            stop_id,
            stop_name,
            stop_lat,
            stop_lon,
            st_geogpoint(stop_lon, stop_lat) as station_geography
        from {{ ref("stops_gtfs") }}
        where feed_start_date = "2025-07-16" and location_type = "1"
    ),

    -- 3. Enriquece cada parada com os dados de sua estação-mãe, se existir.
    -- Esta CTE prepara os dados para a agregação.
    stops_with_station_info as (
        select
            s.stop_id as original_stop_id,

            -- Seleciona os dados da ESTAÇÃO se ela existir, senão usa os dados da
            -- própria PARADA.
            -- Isso cria os campos "representativos" pelos quais vamos agrupar.
            coalesce(st.stop_id, s.stop_id) as representative_stop_id,
            coalesce(st.stop_name, s.stop_name) as representative_stop_name,
            st_astext(
                coalesce(st.station_geography, s.stop_geography)
            ) as representative_geography

        from stops_base as s
        left join stations as st on s.parent_station = st.stop_id
    ),

    -- 4. Agregação final para criar a lista de paradas representadas
    stops_with_station_info_agg as (

        select
            -- Os campos que definem o grupo
            representative_stop_id,
            representative_stop_name,
            representative_geography,

            -- NOVO CAMPO: Cria um array com todos os stop_ids originais do grupo
            array_agg(
                original_stop_id order by original_stop_id
            ) as represented_original_stop_ids

        from stops_with_station_info
        group by
            -- Agrupamos pelos campos representativos para consolidar as linhas
            representative_stop_id, representative_stop_name, representative_geography
    )
select
    * except (representative_geography),
    st_geogfromtext(representative_geography) as representative_geography,
    representative_geography as wkt_stop
from stops_with_station_info_agg
