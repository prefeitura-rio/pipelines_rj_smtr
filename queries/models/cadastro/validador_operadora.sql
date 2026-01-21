{{
    config(
        materialized="view",
        alias="validador_operadora",
    )
}}

select *
from {{ source("cadastro", "validador_operadora") }}
where modo = "Ônibus"
