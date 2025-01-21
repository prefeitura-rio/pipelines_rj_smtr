{{
    config(
        materialized="table",
    )
}}

select
    safe_cast(servico as string) as servico,
    safe_cast(codigo_tecnologia as string) as codigo_tecnologia
from {{ source("planejamento", "tecnologia_servico") }}
