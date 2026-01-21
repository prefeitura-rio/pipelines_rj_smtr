{{
    config(
        materialized="view",
        alias="validador_operadora",
    )
}}

select id_validador, operadora
from {{ source("cadastro", "validador_operadora") }}
where modo = "Ônibus"
