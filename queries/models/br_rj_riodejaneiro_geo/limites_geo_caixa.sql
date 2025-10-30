{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("limites_caixa") }}
