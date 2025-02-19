{{
    config(
        alias="tecnologia_servico",
    )
}}

select
    safe_cast(servico as string) as servico,
    safe_cast(modo as string) as modo,
    safe_cast(codigo_tecnologia as string) as codigo_tecnologia,
    safe_cast(data_inicio_vigencia as string) as data_inicio_vigencia,
    safe_cast(data_fim_vigencia as string) as data_fim_vigencia,
from {{ source("planejamento_staging", "tecnologia_servico") }}
