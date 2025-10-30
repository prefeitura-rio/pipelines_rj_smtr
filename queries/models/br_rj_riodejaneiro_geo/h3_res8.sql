{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("h3_res8_table") }}
