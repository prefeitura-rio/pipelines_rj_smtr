{{ config(alias="cliente", tags=["identificacao"]) }}

with
    cliente as (
        select
            data,
            safe_cast(cd_cliente as string) as cd_cliente,
            timestamp_captura,
            safe_cast(
                json_value(content, '$.CD_TIPO_DOCUMENTO') as string
            ) as cd_tipo_documento,
            safe_cast(json_value(content, '$.NM_CLIENTE') as string) as nm_cliente,
            safe_cast(
                json_value(content, '$.NM_CLIENTE_SOCIAL') as string
            ) as nm_cliente_social,
            safe_cast(
                json_value(content, '$.IN_TIPO_PESSOA_FISICA_JURIDICA') as string
            ) as in_tipo_pessoa_fisica_juridica,
            safe_cast(json_value(content, '$.NR_DOCUMENTO') as string) as nr_documento,
            safe_cast(
                json_value(content, '$.NR_DOCUMENTO_ALTERNATIVO') as string
            ) as nr_documento_alternativo,
            safe_cast(json_value(content, '$.TX_EMAIL') as string) as tx_email,
            safe_cast(json_value(content, '$.NR_TELEFONE') as string) as nr_telefone,
            safe_cast(json_value(content, '$.DT_CADASTRO') as string) as dt_cadastro
        from {{ source("source_jae", "cliente") }}
    ),
    cliente_rn as (
        select
            *,
            row_number() over (
                partition by cd_cliente order by timestamp_captura desc
            ) as rn
        from cliente
    )
select * except (rn)
from cliente_rn
where rn = 1
