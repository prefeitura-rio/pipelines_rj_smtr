{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("polygon_garagem") }}
