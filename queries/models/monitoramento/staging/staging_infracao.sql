{{ config(materialized="view", alias="infracao") }}


select
    data,
    safe_cast(json_value(content, '$.id_infracao') as string) id_infracao,
    safe_cast(json_value(content, '$.modo') as string) modo,
    safe_cast(json_value(content, '$.servico') as string) servico,
    safe_cast(json_value(content, '$.permissao') as string) permissao,
    safe_cast(json_value(content, '$.placa') as string) placa,
    safe_cast(id_auto_infracao as string) id_auto_infracao,
    parse_date(
        "%d/%m/%Y",
        split(safe_cast(json_value(content, '$.data_infracao') as string), " ")[
            offset(0)
        ]
    ) data_infracao,
    parse_datetime(
        '%d/%m/%Y %H:%M:%S',
        if(
            regexp_contains(
                json_value(content, '$.data_infracao'), r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}'
            )
            or data >= "2025-06-25",
            concat(safe_cast(json_value(content, '$.data_infracao') as string), ':00'),
            null
        )
    ) as datetime_infracao,
    safe_cast(json_value(content, '$.infracao') as string) infracao,
    safe_cast(json_value(content, '$.valor') as float64) valor,
    safe_cast(json_value(content, '$.status') as string) status,
    if(
        json_value(content, '$.data_pagamento') = "",
        null,
        parse_date("%d/%m/%Y", json_value(content, '$.data_pagamento'))
    ) data_pagamento,
    safe_cast(
        datetime(
            timestamp_trunc(timestamp(timestamp_captura), second), "America/Sao_Paulo"
        ) as datetime
    ) timestamp_captura
from {{ source("veiculo_staging", "infracao") }} as t
