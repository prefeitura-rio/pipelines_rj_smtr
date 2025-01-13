{{
    config(
        alias="ordem_ressarcimento",
    )
}}

with
    ordem_ressarcimento as (
        select
            data,
            safe_cast(id as string) as id_ordem_ressarcimento,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez',
                    safe_cast(json_value(content, '$.data_inclusao') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            parse_date(
                '%Y-%m-%d', safe_cast(json_value(content, '$.data_ordem') as string)
            ) as data_ordem,
            safe_cast(json_value(content, '$.id_consorcio') as string) as id_consorcio,
            safe_cast(json_value(content, '$.id_linha') as string) as id_linha,
            safe_cast(json_value(content, '$.id_operadora') as string) as id_operadora,
            safe_cast(
                json_value(content, '$.id_ordem_pagamento') as string
            ) as id_ordem_pagamento,
            safe_cast(
                json_value(content, '$.id_ordem_pagamento_consorcio') as string
            ) as id_ordem_pagamento_consorcio,
            safe_cast(
                json_value(
                    content, '$.id_ordem_pagamento_consorcio_operadora'
                ) as string
            ) as id_ordem_pagamento_consorcio_operadora,
            safe_cast(
                json_value(content, '$.id_status_ordem') as string
            ) as id_status_ordem,
            safe_cast(json_value(content, '$.qtd_debito') as integer) as qtd_debito,
            safe_cast(
                json_value(content, '$.qtd_gratuidade') as integer
            ) as qtd_gratuidade,
            safe_cast(
                json_value(content, '$.qtd_integracao') as integer
            ) as qtd_integracao,
            safe_cast(
                json_value(content, '$.qtd_rateio_credito') as integer
            ) as qtd_rateio_credito,
            safe_cast(
                json_value(content, '$.qtd_rateio_debito') as integer
            ) as qtd_rateio_debito,
            safe_cast(
                json_value(content, '$.qtd_vendaabordo') as integer
            ) as qtd_vendaabordo,
            safe_cast(json_value(content, '$.valor_bruto') as numeric) as valor_bruto,
            safe_cast(json_value(content, '$.valor_debito') as numeric) as valor_debito,
            safe_cast(
                json_value(content, '$.valor_gratuidade') as numeric
            ) as valor_gratuidade,
            safe_cast(
                json_value(content, '$.valor_integracao') as numeric
            ) as valor_integracao,
            safe_cast(
                json_value(content, '$.valor_liquido') as numeric
            ) as valor_liquido,
            safe_cast(
                json_value(content, '$.valor_rateio_credito') as numeric
            ) as valor_rateio_credito,
            safe_cast(
                json_value(content, '$.valor_rateio_debito') as numeric
            ) as valor_rateio_debito,
            safe_cast(json_value(content, '$.valor_taxa') as numeric) as valor_taxa,
            safe_cast(
                json_value(content, '$.valor_vendaabordo') as numeric
            ) as valor_vendaabordo
        from {{ source("source_jae", "ordem_ressarcimento") }}
    ),
    ordem_ressarcimento_rn as (
        select
            *,
            row_number() over (
                partition by id_ordem_ressarcimento order by timestamp_captura desc
            ) as rn
        from ordem_ressarcimento
    )
select * except (rn)
from ordem_ressarcimento_rn
where rn = 1
