{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}
{% if var("start_date") < var("DATA_GTFS_V4_INICIO") %}
    select *
    from {{ ref("viagens_remuneradas_v1") }}
    where
        data < date("{{ var('DATA_GTFS_V4_INICIO') }}")
    union all by name
{% endif %}
SELECT *
FROM {{ ref("viagens_remuneradas_v2") }}
WHERE
  data >= date("{{ var('DATA_GTFS_V4_INICIO') }}")
