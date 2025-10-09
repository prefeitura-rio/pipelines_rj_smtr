{{
    config(
        alias="ordem_pagamento_cct",
    )
}}


select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    timestamp_captura,
    parse_date(
        '%Y-%m-%d', safe_cast(json_value(content, '$.dataOrdem') as string)
    ) as dataordem,
    safe_cast(json_value(content, '$.nomeConsorcio') as string) as nomeconsorcio,
    safe_cast(json_value(content, '$.nomeOperadora') as string) as nomeoperadora,
    replace(safe_cast(json_value(content, '$.userId') as string), ".0", "") as userid,
    safe_cast(json_value(content, '$.valor') as numeric) as valor,
    replace(
        safe_cast(json_value(content, '$.idOrdemPagamento') as string), ".0", ""
    ) as idordempagamento,
    replace(
        safe_cast(json_value(content, '$.idOperadora') as string), ".0", ""
    ) as idoperadora,
    replace(
        safe_cast(json_value(content, '$.operadoraCpfCnpj') as string), ".0", ""
    ) as operadoracpfcnpj,
    replace(
        safe_cast(json_value(content, '$.idConsorcio') as string), ".0", ""
    ) as idconsorcio,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.bqUpdatedAt') as string)
        ),
        "America/Sao_Paulo"
    ) as bqupdatedat,
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
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.dataCaptura') as string)
        ),
        "America/Sao_Paulo"
    ) as datacaptura,
    replace(
        safe_cast(json_value(content, '$.ordemPagamentoAgrupadoId') as string), ".0", ""
    ) as ordempagamentoagrupadoid
from {{ source("source_cct", "ordem_pagamento") }}
