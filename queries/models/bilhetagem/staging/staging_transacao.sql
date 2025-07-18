{{
    config(
        alias="transacao",
    )
}}

select
    data,
    hora,
    id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.assinatura') as string) as assinatura,
    safe_cast(json_value(content, '$.cd_aplicacao') as string) as cd_aplicacao,
    safe_cast(json_value(content, '$.cd_emissor') as string) as cd_emissor,
    safe_cast(json_value(content, '$.cd_consorcio') as string) as cd_consorcio,
    safe_cast(json_value(content, '$.cd_linha') as string) as cd_linha,
    safe_cast(
        json_value(content, '$.cd_matriz_integracao') as string
    ) as cd_matriz_integracao,
    safe_cast(json_value(content, '$.cd_operadora') as string) as cd_operadora,
    safe_cast(json_value(content, '$.cd_secao') as string) as cd_secao,
    safe_cast(
        json_value(content, '$.cd_status_transacao') as string
    ) as cd_status_transacao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_processamento') as string)
        ),
        "America/Sao_Paulo"
    ) as data_processamento,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_transacao') as string)
        ),
        "America/Sao_Paulo"
    ) as data_transacao,
    replace(
        safe_cast(json_value(content, '$.id_cliente') as string), ".0", ""
    ) as id_cliente,
    replace(
        safe_cast(json_value(content, '$.id_produto') as string), ".0", ""
    ) as id_produto,
    safe_cast(json_value(content, '$.id_servico') as string) as id_servico,
    safe_cast(json_value(content, '$.id_tipo_midia') as string) as id_tipo_midia,
    safe_cast(json_value(content, '$.is_abt') as bool) as is_abt,
    safe_cast(json_value(content, '$.latitude_trx') as float64) as latitude_trx,
    safe_cast(json_value(content, '$.longitude_trx') as float64) as longitude_trx,
    safe_cast(
        json_value(content, '$.nr_logico_midia_operador') as string
    ) as nr_logico_midia_operador,
    safe_cast(
        json_value(content, '$.numero_serie_validador') as string
    ) as numero_serie_validador,
    safe_cast(json_value(content, '$.pan_hash') as string) as pan_hash,
    safe_cast(
        json_value(content, '$.posicao_validador') as string
    ) as posicao_validador,
    safe_cast(json_value(content, '$.sentido') as string) as sentido,
    safe_cast(json_value(content, '$.tipo_integracao') as string) as tipo_integracao,
    safe_cast(json_value(content, '$.tipo_transacao') as string) as tipo_transacao,
    safe_cast(json_value(content, '$.uid_origem') as string) as uid_origem,
    safe_cast(json_value(content, '$.valor_tarifa') as float64) as valor_tarifa,
    safe_cast(json_value(content, '$.valor_transacao') as float64) as valor_transacao,
    safe_cast(json_value(content, '$.veiculo_id') as string) as veiculo_id,
    safe_cast(json_value(content, '$.vl_saldo') as float64) as vl_saldo,
    safe_cast(json_value(content, '$.id_tipo_modal') as string) as id_tipo_modal
from {{ source("source_jae", "transacao") }}
