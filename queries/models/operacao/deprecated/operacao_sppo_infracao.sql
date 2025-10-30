{{
    config(
        materialized="view",
        alias="sppo_infracao",
    )
}}

select
    data,
    timestamp_captura,
    safe_cast(placa as string) placa,
    safe_cast(id_auto_infracao as string) id_auto_infracao,
    safe_cast(json_value(content, '$.permissao') as string) permissao,
    safe_cast(json_value(content, '$.modo') as string) modo,
    parse_date(
        "%d/%m/%Y", safe_cast(json_value(content, '$.data_infracao') as string)
    ) data_infracao,
    safe_cast(json_value(content, '$.valor') as float64) valor,
    safe_cast(json_value(content, '$.id_infracao') as string) id_infracao,
    safe_cast(json_value(content, '$.infracao') as string) infracao,
    safe_cast(json_value(content, '$.status') as string) status,
    parse_date(
        "%d/%m/%Y", safe_cast(json_value(content, '$.data_pagamento') as string)
    ) data_pagamento
from `rj-smtr-staging.operacao_staging.sppo_infracao` as t
