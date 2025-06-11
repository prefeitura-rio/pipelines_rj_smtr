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
        where
            data
            between "{{ var('data_inicio_trips_shapes') }}"
            and '{{var("DATA_GTFS_V2_INICIO") }}'

        union all

        select *
        from {{ ref("aux_ordem_servico_diaria_v3") }}
        where data > '{{var("DATA_GTFS_V2_INICIO") }}'
    )
select *
from ordem_servico_diaria
where viagens_planejadas > 0
