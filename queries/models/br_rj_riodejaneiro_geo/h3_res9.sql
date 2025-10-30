{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("h3_res9_table") }}
