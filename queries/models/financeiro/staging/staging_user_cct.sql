{{
    config(
        alias="user_cct",
    )
}}


select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    timestamp_captura,
    safe_cast(json_value(content, '$.email') as string) as email,
    safe_cast(json_value(content, '$.provider') as string) as provider,
    safe_cast(json_value(content, '$.socialId') as string) as socialid,
    safe_cast(json_value(content, '$.fullName') as string) as fullname,
    safe_cast(json_value(content, '$.firstName') as string) as firstname,
    safe_cast(json_value(content, '$.lastName') as string) as lastname,
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
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.deletedAt') as string)
        ),
        "America/Sao_Paulo"
    ) as deletedat,
    replace(
        safe_cast(json_value(content, '$.permitCode') as string), ".0", ""
    ) as permitcode,
    replace(safe_cast(json_value(content, '$.cpfCnpj') as string), ".0", "") as cpfcnpj,
    replace(
        safe_cast(json_value(content, '$.bankCode') as string), ".0", ""
    ) as bankcode,
    replace(
        safe_cast(json_value(content, '$.bankAgency') as string), ".0", ""
    ) as bankagency,
    replace(
        safe_cast(json_value(content, '$.bankAccount') as string), ".0", ""
    ) as bankaccount,
    replace(
        safe_cast(json_value(content, '$.bankAccountDigit') as string), ".0", ""
    ) as bankaccountdigit,
    safe_cast(json_value(content, '$.phone') as string) as phone,
    safe_cast(json_value(content, '$.isSgtuBlocked') as bool) as issgtublocked,
    safe_cast(json_value(content, '$.passValidatorId') as string) as passvalidatorid,
    replace(
        safe_cast(json_value(content, '$.previousBankCode') as string), ".0", ""
    ) as previousbankcode,
    safe_cast(json_value(content, '$.bloqueado') as bool) as bloqueado
from {{ source("source_cct", "user") }}
