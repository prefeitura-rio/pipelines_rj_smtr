{{
    config(
        materialized="view",
        alias="sppo_infracao",
    )
}}

select
    data,
    timestamp_captura,
    safe_cast(placa as string) as placa,
    safe_cast(id_auto_infracao as string) as id_auto_infracao,
    safe_cast(json_value(content, '$.permissao') as string) as permissao,
    safe_cast(json_value(content, '$.modo') as string) as modo,
    parse_date(
        "%d/%m/%Y", safe_cast(json_value(content, '$.data_infracao') as string)
    ) as data_infracao,
    safe_cast(json_value(content, '$.valor') as float64) as valor,
    safe_cast(json_value(content, '$.id_infracao') as string) as id_infracao,
    safe_cast(json_value(content, '$.infracao') as string) as infracao,
    safe_cast(json_value(content, '$.status') as string) as status,
    parse_date(
        "%d/%m/%Y", safe_cast(json_value(content, '$.data_pagamento') as string)
    ) as data_pagamento
from `rj-smtr-staging.operacao_staging.sppo_infracao` as t
