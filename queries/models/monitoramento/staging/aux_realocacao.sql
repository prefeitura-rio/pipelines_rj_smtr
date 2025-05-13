{{ config(alias=this.name ~ "_" ~ var("fonte_gps")) }}

select
    data,
    safe_cast(hora as int64) hora,
    safe_cast(
        datetime(
            timestamp(safe_cast(json_value(content, '$.datetime_operacao') as string)),
            "America/Sao_Paulo"
        ) as datetime
    ) datetime_operacao,
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
    safe_cast(
        datetime(
            timestamp(safe_cast(json_value(content, '$.datetime_entrada') as string)),
            "America/Sao_Paulo"
        ) as datetime
    ) as datetime_entrada,
    safe_cast(
        datetime(
            timestamp(
                nullif(safe_cast(json_value(content, '$.datetime_saida') as string), "")
            ),
            "America/Sao_Paulo"
        ) as datetime
    ) as datetime_saida,
    safe_cast(
        datetime(timestamp(datetime_processamento), "America/Sao_Paulo") as datetime
    ) as datetime_processamento,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) as datetime_captura
from
    {{
        source(
            "source_" ~ var("fonte_gps"),
            "realocacao",
        )
    }}
