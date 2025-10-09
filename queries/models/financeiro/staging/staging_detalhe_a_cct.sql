{{
    config(
        alias="detalhe_a_cct",
    )
}}


select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.dataVencimento') as string)
        ),
        "America/Sao_Paulo"
    ) as datavencimento,
    safe_cast(json_value(content, '$.tipoMoeda') as string) as tipomoeda,
    safe_cast(
        json_value(content, '$.indicadorBloqueio') as string
    ) as indicadorbloqueio,
    safe_cast(
        json_value(content, '$.indicadorFormaParcelamento') as string
    ) as indicadorformaparcelamento,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.periodoVencimento') as string)
        ),
        "America/Sao_Paulo"
    ) as periodovencimento,
    case
        when length(safe_cast(json_value(content, '$.dataEfetivacao') as string)) = 32
        then
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E6S%Ez',
                    safe_cast(json_value(content, '$.dataEfetivacao') as string)
                ),
                "America/Sao_Paulo"
            )
        else
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.dataEfetivacao') as string)
                ),
                "America/Sao_Paulo"
            )
    end as dataefetivacao,
    replace(safe_cast(json_value(content, '$.nsr') as string), ".0", "") as nsr,
    replace(
        safe_cast(json_value(content, '$.headerLoteId') as string), ".0", ""
    ) as headerloteid,
    safe_cast(json_value(content, '$.valorLancamento') as numeric) as valorlancamento,
    safe_cast(
        json_value(content, '$.valorRealEfetivado') as numeric
    ) as valorrealefetivado,
    safe_cast(json_value(content, '$.quantidadeMoeda') as numeric) as quantidademoeda,
    safe_cast(json_value(content, '$.finalidadeDOC') as string) as finalidadedoc,
    replace(
        safe_cast(json_value(content, '$.numeroDocumentoEmpresa') as string), ".0", ""
    ) as numerodocumentoempresa,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.createdAt') as string)
        ),
        "America/Sao_Paulo"
    ) as createdat,
    replace(
        safe_cast(json_value(content, '$.loteServico') as string), ".0", ""
    ) as loteservico,
    replace(
        safe_cast(json_value(content, '$.quantidadeParcelas') as string), ".0", ""
    ) as quantidadeparcelas,
    replace(
        safe_cast(json_value(content, '$.numeroParcela') as string), ".0", ""
    ) as numeroparcela,
    safe_cast(
        json_value(content, '$.numeroDocumentoBanco') as string
    ) as numerodocumentobanco,
    replace(
        safe_cast(json_value(content, '$.itemTransacaoAgrupadoId') as string), ".0", ""
    ) as itemtransacaoagrupadoid,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.updatedAt') as string)
        ),
        "America/Sao_Paulo"
    ) as updatedat,
    safe_cast(json_value(content, '$.ocorrenciasCnab') as string) as ocorrenciascnab,
    safe_cast(json_value(content, '$.retornoName') as string) as retornoname,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.retornoDatetime') as string)
        ),
        "America/Sao_Paulo"
    ) as retornodatetime,
    replace(
        safe_cast(json_value(content, '$.ordemPagamentoAgrupadoHistoricoId') as string),
        ".0",
        ""
    ) as ordempagamentoagrupadohistoricoid
from {{ source("source_cct", "detalhe_a") }}
