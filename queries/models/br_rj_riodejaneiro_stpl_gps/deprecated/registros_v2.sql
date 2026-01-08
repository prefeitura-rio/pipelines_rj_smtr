{{
    config(
        materialized="view",
    )
}}

select
    safe_cast(codigo as string) id_veiculo,
    safe_cast(
        datetime(timestamp(datahora), "America/Sao_Paulo") as datetime
    ) timestamp_gps,
    safe_cast(
        datetime(
            timestamp_trunc(timestamp(timestamp_captura), second), "America/Sao_Paulo"
        ) as datetime
    ) timestamp_captura,
    replace(content, "None", "") content,
    safe_cast(data as date) data,
    safe_cast(hora as int64) hora
from `rj-smtr-staging.br_rj_riodejaneiro_stpl_gps_staging.registros`
