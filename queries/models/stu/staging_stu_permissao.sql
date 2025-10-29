{{ config(alias="permissao") }}

select
    tptran as id_tipo_transporte,
    tpperm as id_tipo_permissao,
    termo as id_termo,
    dv as digito_verificador_termo,
    safe_cast(json_value(content, '$.proaut') as string) as processo_autorizacao,
    safe_cast(json_value(content, '$.dtauto') as datetime) as data_autorizacao,
    safe_cast(json_value(content, '$.procanc') as string) as processo_cancelamento,
    safe_cast(json_value(content, '$.dtcanc') as datetime) as data_cancelamento,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.motivo') as string) as motivo,
    safe_cast(json_value(content, '$.sorteio') as boolean) as sorteio,
    safe_cast(json_value(content, '$.dnm') as boolean) as dnm,
    safe_cast(json_value(content, '$.origem') as string) as origem,
    safe_cast(json_value(content, '$.intransferivel') as boolean) as intransferivel,
    safe_cast(json_value(content, '$.sem_auxiliar') as boolean) as sem_auxiliar,
    safe_cast(json_value(content, '$.id_artigo') as int64) as id_artigo,
    safe_cast(json_value(content, '$.codigoHash') as string) as codigo_hash,
    safe_cast(
        json_value(content, '$._datetime_execucao_flow') as datetime
    ) as datetime_execucao_flow,
    timestamp_captura
from {{ source("source_stu", "permissao") }}
