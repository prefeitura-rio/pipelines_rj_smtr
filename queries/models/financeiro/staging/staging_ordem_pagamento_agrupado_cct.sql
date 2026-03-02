{{
    config(
        alias="ordem_pagamento_agrupado_cct",
    )
}}


select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.dataPagamento') as string)
        ),
        "America/Sao_Paulo"
    ) as datapagamento,
    safe_cast(json_value(content, '$.valorTotal') as numeric) as valortotal,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.createdAt') as string)
        ),
        "America/Sao_Paulo"
    ) as createdat,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.updatedAt') as string)
        ),
        "America/Sao_Paulo"
    ) as updatedat,
    replace(
        safe_cast(json_value(content, '$.pagadorId') as string), ".0", ""
    ) as pagadorid,
    replace(
        safe_cast(json_value(content, '$.ordemPagamentoAgrupadoId') as string), ".0", ""
    ) as ordempagamentoagrupadoid,
from {{ source("source_cct", "ordem_pagamento_agrupado") }}
