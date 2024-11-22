{{
    config(
        alias="transacao_ordem",
    )
}}

select
    data,
    hora,
    id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    id_ordem_ressarcimento,
    datetime(
        parse_timestamp('%Y-%m-%dT%H:%M:%E6S%Ez', data_processamento),
        "America/Sao_Paulo"
    ) as data_processamento,
    datetime(
        parse_timestamp('%Y-%m-%dT%H:%M:%E6S%Ez', data_transacao), "America/Sao_Paulo"
    ) as data_transacao
from {{ source("source_jae", "transacao_ordem") }}
