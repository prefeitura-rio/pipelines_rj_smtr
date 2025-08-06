{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}
select *
from {{ ref("viagens_remuneradas_v1") }}
where data < date("{{ var('DATA_GTFS_V4_INICIO') }}") 
full outer union all by name
select *
from {{ ref("viagens_remuneradas_v2") }}
where data >= date("{{ var('DATA_GTFS_V4_INICIO') }}")
