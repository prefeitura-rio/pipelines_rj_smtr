{{
    config(
        alias="gratuidade",
    )
}}

select
    data,
    safe_cast(id as string) as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.cd_cliente') as string) as cd_cliente,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    safe_cast(
        json_value(content, '$.id_status_gratuidade') as string
    ) as id_status_gratuidade,
    safe_cast(
        json_value(content, '$.id_tipo_gratuidade') as string
    ) as id_tipo_gratuidade,
    safe_cast(json_value(content, '$.tipo_gratuidade') as string) as tipo_gratuidade,
    cast(
        cast(
            cast(
                safe_cast(
                    json_value(content, '$.deficiencia_permanente') as string
                ) as float64
            ) as integer
        ) as bool
    ) as deficiencia_permanente,
    safe_cast(json_value(content, '$.rede_ensino') as string) as rede_ensino,

from {{ source("source_jae", "gratuidade") }}
