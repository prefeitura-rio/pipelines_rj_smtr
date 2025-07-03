{{
    config(
        alias="endereco",
    )
}}

with
    endereco as (
        select
            data,
            safe_cast(nr_seq_endereco as string) as nr_seq_endereco,
            timestamp_captura,
            safe_cast(json_value(content, '$.CD_CLIENTE') as string) as cd_cliente,
            safe_cast(
                json_value(content, '$.CD_TIPO_ENDERECO') as string
            ) as cd_tipo_endereco,
            safe_cast(
                json_value(content, '$.CD_TIPO_LOGRADOURO') as string
            ) as cd_tipo_logradouro,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as dt_inclusao,
            safe_cast(json_value(content, '$.NM_BAIRRO') as string) as nm_bairro,
            safe_cast(json_value(content, '$.NM_CIDADE') as string) as nm_cidade,
            safe_cast(json_value(content, '$.NR_CEP') as string) as nr_cep,
            safe_cast(
                json_value(content, '$.NR_LOGRADOURO') as string
            ) as nr_logradouro,
            safe_cast(json_value(content, '$.SG_UF') as string) as sg_uf,
            safe_cast(
                json_value(content, '$.TX_COMPLEMENTO_LOGRADOURO') as string
            ) as tx_complemento_logradouro,
            safe_cast(json_value(content, '$.TX_LOGRADOURO') as string) as tx_logradouro
        from {{ source("source_jae", "endereco") }}
    ),
    endereco_rn as (
        select
            *,
            row_number() over (
                partition by nr_seq_endereco order by timestamp_captura desc
            ) as rn
        from endereco
    )
select * except (rn)
from endereco_rn
where rn = 1
