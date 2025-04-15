{{
    config(
        materialized="table",
    )
}}

with tecnologia as (select * from {{ ref("staging_tecnologia_servico") }})
select
    parse_date('%d/%m/%Y', inicio_vigencia) as inicio_vigencia,
    parse_date('%d/%m/%Y', fim_vigencia) as fim_vigencia,
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
    end as menor_tecnologia_permitida
from tecnologia
