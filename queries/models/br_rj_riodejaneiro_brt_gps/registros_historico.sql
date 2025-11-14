{{
    config(
        materialized="view",
    )
}}

select
    safe_cast(codigo as string) codigo,
    safe_cast(placa as string) placa,
    safe_cast(linha as string) linha,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(
        datetime(timestamp(timestamp_gps), "America/Sao_Paulo") as datetime
    ) timestamp_gps,
    safe_cast(velocidade as int64) velocidade,
    safe_cast(id_migracao_trajeto as string) id_migracao_trajeto,
    safe_cast(sentido as string) sentido,
    safe_cast(trajeto as string) trajeto,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) timestamp_captura,
    safe_cast(data as date) data,
    safe_cast(hora as int64) hora
from {{ source("br_rj_riodejaneiro_brt_gps_staging", "registros_historico") }} as t
