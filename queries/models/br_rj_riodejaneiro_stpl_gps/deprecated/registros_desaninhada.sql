{{
    config(
        materialized="view",
    )
}}

select
    data,
    hora,
    id_veiculo,
    timestamp_captura,
    timestamp_gps,
    safe_cast(json_value(content, "$.latitude") as float64) latitude,
    safe_cast(json_value(content, "$.longitude") as float64) longitude,
    json_value(content, "$.linha") linha,
    safe_cast(json_value(content, '$.velocidade') as float64) as velocidade,
    json_value(content, '$.id_migracao_trajeto') as id_migracao_trajeto,
    json_value(content, '$.sentido') as sentido,
    json_value(content, '$.trajeto') as trajeto,
    json_value(content, '$.hodometro') as hodometro
from `rj-smtr.br_rj_riodejaneiro_stpl_gps.registros_v2`
