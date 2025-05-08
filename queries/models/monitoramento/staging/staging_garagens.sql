{{
    config(
        alias="garagens",
    )
}}

select
    nullif(trim(safe_cast(operador as string)), "") as operador,
    nullif(trim(safe_cast(endereco as string)), "") as endereco,
    nullif(trim(safe_cast(bairro as string)), "") as bairro,
    case
        when safe_cast(oficial as string) = "SIM"
        then true
        when safe_cast(oficial as string) = "NÃO"
        then false
        else null
    end as oficial,
    case
        when safe_cast(ativa_ou_inativa as string) = "ATIVA"
        then true
        when safe_cast(ativa_ou_inativa as string) = "INATIVA"
        then false
        else null
    end as ativa,
    lower(nullif(trim(safe_cast(uso as string)), "")) as uso,
    nullif(trim(safe_cast(obs as string)), "") as observacao,
    safe_cast(area_m2 as string) as area_m2,
    safe_cast(geometry_wkt as string) as geometry_wkt
from {{ source("monitoramento_staging", "garagens") }}
