{{
    config(
        alias="tecnologia_servico",
    )
}}

select
    safe_cast(inicio_vigencia as string) as inicio_vigencia,
    safe_cast(fim_vigencia as string) as fim_vigencia,
    safe_cast(linha as string) as servico,
    safe_cast(null as string) as modo,
    safe_cast(codigo_tecnologia as string) as codigo_tecnologia
from {{ source("planejamento_staging", "tecnologia_servico") }}
