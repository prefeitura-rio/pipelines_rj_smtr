{{
    config(
        alias="ordem_pagamento_agrupado_historico_cct",
    )
}}


select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.dataReferencia') as string)
        ),
        "America/Sao_Paulo"
    ) as datareferencia,
    replace(
        safe_cast(json_value(content, '$.userBankCode') as string), ".0", ""
    ) as userbankcode,
    replace(
        safe_cast(json_value(content, '$.userBankAgency') as string), ".0", ""
    ) as userbankagency,
    replace(
        safe_cast(json_value(content, '$.userBankAccount') as string), ".0", ""
    ) as userbankaccount,
    replace(
        safe_cast(json_value(content, '$.userBankAccountDigit') as string), ".0", ""
    ) as userbankaccountdigit,
    replace(
        safe_cast(json_value(content, '$.statusRemessa') as string), ".0", ""
    ) as statusremessa,
    safe_cast(
        json_value(content, '$.motivoStatusRemessa') as string
    ) as motivostatusremessa,
    replace(
        safe_cast(json_value(content, '$.ordemPagamentoAgrupadoId') as string), ".0", ""
    ) as ordempagamentoagrupadoid,
from {{ source("source_cct", "ordem_pagamento_agrupado_historico") }}
