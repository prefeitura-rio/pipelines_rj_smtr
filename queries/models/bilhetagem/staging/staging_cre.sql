{{
    config(
        alias="cre",
    )
}}

select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.tx_endereco') as string) as tx_endereco,
    safe_cast(json_value(content, '$.nm_bairro') as string) as nm_bairro,
    safe_cast(json_value(content, '$.nr_cep') as string) as nr_cep,
    safe_cast(json_value(content, '$.nm_coordenador') as string) as nm_coordenador,
    safe_cast(json_value(content, '$.email_cre') as string) as email_cre,
    safe_cast(json_value(content, '$.tx_telefone') as string) as tx_telefone,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    safe_cast(json_value(content, '$.nr_logradouro') as string) as nr_logradouro,
    safe_cast(json_value(content, '$.nm_cre') as string) as nm_cre,
    safe_cast(json_value(content, '$.nm_cidade') as string) as nm_cidade,
    safe_cast(json_value(content, '$.sg_uf') as string) as sg_uf
from {{ source("source_jae_dev", "cre") }}
qualify row_number() over (partition by id order by timestamp_captura desc) = 1
