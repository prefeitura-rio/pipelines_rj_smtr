{{ config(alias=this.name ~ "_" ~ var("fonte_gps")) }}

select
    data,
    safe_cast(hora as int64) hora,
    safe_cast(
        datetime(
            timestamp(safe_cast(json_value(content, '$.datetime') as string)),
            "America/Sao_Paulo"
        ) as datetime
    ) datetime_gps,
    safe_cast(id_veiculo as string) id_veiculo,
    concat(
        ifnull(
            regexp_extract(
                safe_cast(json_value(content, '$.servico') as string), r'[A-Z]+'
            ),
            ""
        ),
        ifnull(
            regexp_extract(
                safe_cast(json_value(content, '$.servico') as string), r'[0-9]+'
            ),
            ""
        )
    ) as servico,
    safe_cast(json_value(content, '$.sentido') as string) sentido,
    safe_cast(replace(json_value(content, '$.latitude'), ',', '.') as float64) latitude,
    safe_cast(
        replace(json_value(content, '$.longitude'), ',', '.') as float64
    ) longitude,
    safe_cast(json_value(content, '$.velocidade') as int64) velocidade,
    safe_cast(json_value(content, '$.direcao') as numeric) direcao,
    safe_cast(json_value(content, '$.route_id') as string) route_id,
    safe_cast(json_value(content, '$.trip_id') as string) trip_id,
    safe_cast(json_value(content, '$.shape_id') as string) shape_id,
    safe_cast(
        datetime(
            timestamp(safe_cast(json_value(content, '$.datetime_envio') as string)),
            "America/Sao_Paulo"
        ) as datetime
    ) datetime_envio,
    safe_cast(
        datetime(
            timestamp(safe_cast(json_value(content, '$.datetime_servidor') as string)),
            "America/Sao_Paulo"
        ) as datetime
    ) datetime_servidor,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) datetime_captura
from
    {{
        source(
            "source_" ~ var("fonte_gps"),
            "registros",
        )
    }}
