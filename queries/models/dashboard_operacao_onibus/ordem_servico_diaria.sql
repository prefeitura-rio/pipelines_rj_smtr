{{
    config(
        materialized="view",
        labels={
            "dashboard": "yes",
        },
    )
}}

select *
from {{ ref("aux_ordem_servico_diaria_v1") }}
where data < "{{ var('data_inicio_trips_shapes') }}"

union all

select *
from {{ ref("aux_ordem_servico_diaria_v2") }}
where data >= "{{ var('data_inicio_trips_shapes') }}"
