{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
select *
from {{ ref("viagem_planejada_v1") }}
where
    data <= date("{{ var('DATA_SUBSIDIO_V6_INICIO') }}")
union all
select *
from {{ ref("viagem_planejada_v2") }}
where
    data > date("{{ var('DATA_SUBSIDIO_V6_INICIO') }}")
