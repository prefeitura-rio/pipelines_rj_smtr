{{
    config(
        alias="percentual_rateio_integracao",
    )
}}

with
    percentual_rateio_integracao as (
        select
            data,
            safe_cast(id as string) as id,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            safe_cast(
                json_value(content, '$.dt_fim_validade') as string
            ) as dt_fim_validade,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:S%Ez',
                    safe_cast(json_value(content, '$.dt_inclusao') as string)
                ),
                'America/Sao_Paulo'
            ) as dt_inclusao,
            date(
                parse_timestamp(
                    '%Y-%m-%d',
                    safe_cast(json_value(content, '$.dt_inicio_validade') as string)
                ),
                'America/Sao_Paulo'
            ) as dt_inicio_validade,
            replace(
                safe_cast(
                    json_value(content, '$.id_tipo_modal_integracao_t1') as string
                ),
                '.0',
                ''
            ) as id_tipo_modal_integracao_t1,
            replace(
                safe_cast(
                    json_value(content, '$.id_tipo_modal_integracao_t2') as string
                ),
                '.0',
                ''
            ) as id_tipo_modal_integracao_t2,
            replace(
                safe_cast(
                    json_value(content, '$.id_tipo_modal_integracao_t3') as string
                ),
                '.0',
                ''
            ) as id_tipo_modal_integracao_t3,
            replace(
                safe_cast(
                    json_value(content, '$.id_tipo_modal_integracao_t4') as string
                ),
                '.0',
                ''
            ) as id_tipo_modal_integracao_t4,
            replace(
                safe_cast(json_value(content, '$.id_tipo_modal_origem') as string),
                '.0',
                ''
            ) as id_tipo_modal_origem,
            safe_cast(
                json_value(content, '$.perc_rateio_integracao_t1') as float64
            ) as perc_rateio_integracao_t1,
            safe_cast(
                json_value(content, '$.perc_rateio_integracao_t2') as float64
            ) as perc_rateio_integracao_t2,
            safe_cast(
                json_value(content, '$.perc_rateio_integracao_t3') as float64
            ) as perc_rateio_integracao_t3,
            safe_cast(
                json_value(content, '$.perc_rateio_integracao_t4') as float64
            ) as perc_rateio_integracao_t4,
            safe_cast(
                json_value(content, '$.perc_rateio_origem') as float64
            ) as perc_rateio_origem
        from {{ source("source_jae", "percentual_rateio_integracao") }}
    )
select * except (rn)
from
    (
        select
            *, row_number() over (partition by id order by timestamp_captura desc) as rn
        from percentual_rateio_integracao
    )
where rn = 1
