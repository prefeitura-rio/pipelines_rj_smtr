{{
    config(
        materialized="table",
    )
}}

with
    tecnologia as (
        select
            safe_cast(servico as string) as servico,
            safe_cast(modo as string) as modo,
            safe_cast(codigo_tecnologia as string) as codigo_tecnologia
        from {{ source("planejamento_staging", "tecnologia_servico") }}
    )
select
    servico,
    modo,
    codigo_tecnologia,
    case
        when substring(codigo_tecnologia, 4, 1) = "1"
        then "PADRON"
        when substring(codigo_tecnologia, 3, 1) = "1"
        then "BASICO"
        when substring(codigo_tecnologia, 2, 1) = "1"
        then "MIDI"
        when substring(codigo_tecnologia, 1, 1) = "1"
        then "MINI"
        else null
    end as maior_tecnologia_permitida,
    case
        when substring(codigo_tecnologia, 1, 1) = "1"
        then "MINI"
        when substring(codigo_tecnologia, 2, 1) = "1"
        then "MIDI"
        when substring(codigo_tecnologia, 3, 1) = "1"
        then "BASICO"
        when substring(codigo_tecnologia, 4, 1) = "1"
        then "PADRON"
        else null
    end as menor_tecnologia_permitida,
-- feed_start_date -- TODO: puxar essa coluna do gtfs
from tecnologia
