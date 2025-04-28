{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("subsidio_viagem_remunerada") }}
where
    data between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    and modo = "Ônibus SPPO"
