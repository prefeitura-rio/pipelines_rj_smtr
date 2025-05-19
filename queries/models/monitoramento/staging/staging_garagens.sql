{{
    config(
        alias="garagens",
    )
}}

with
    garagens as (
        select
            data as inicio_vigencia,
            nullif(trim(safe_cast(operador as string)), "") as operador,
            nullif(trim(safe_cast(endereco as string)), "") as endereco,
            nullif(trim(safe_cast(bairro as string)), "") as bairro,
            case
                when upper(trim(safe_cast(oficial as string))) = "SIM"
                then true
                when upper(trim(safe_cast(oficial as string))) = "NÃO"
                then false
                else null
            end as oficial,
            case
                when upper(trim(safe_cast(ativa_ou_inativa as string))) = "ATIVA"
                then true
                when upper(trim(safe_cast(ativa_ou_inativa as string))) = "INATIVA"
                then false
                else null
            end as ativa,
            lower(nullif(trim(safe_cast(uso as string)), "")) as uso,
            nullif(trim(safe_cast(obs as string)), "") as observacao,
            safe_cast(area_m2 as string) as area_m2,
            safe_cast(geometry_wkt as string) as geometry_wkt
        from {{ source("monitoramento_staging", "garagens") }}
    )

select
    inicio_vigencia,
    date_sub(
        lead(inicio_vigencia) over (
            partition by operador, endereco order by inicio_vigencia
        ),
        interval 1 day
    ) as fim_vigencia,
    operador,
    endereco,
    bairro,
    oficial,
    ativa,
    uso,
    observacao,
    area_m2,
    geometry_wkt
from garagens
