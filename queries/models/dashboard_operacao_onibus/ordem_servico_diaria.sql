{{
    config(
        materialized="view",
        labels={
            "dashboard": "yes",
        },
    )
}}

with
    ordem_servico_diaria as (
        select *
        from {{ ref("aux_ordem_servico_diaria_v1") }}
        where data < "{{ var('data_inicio_trips_shapes') }}"

        union all

        select *
        from {{ ref("aux_ordem_servico_diaria_v2") }}
        where data >= "{{ var('data_inicio_trips_shapes') }}"
    )
select *
from ordem_servico_diaria
where viagens_planejadas > 0
