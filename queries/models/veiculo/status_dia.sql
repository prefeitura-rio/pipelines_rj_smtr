{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_veiculo"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    sppo as (
        select
            data,
            id_veiculo,
            "Ônibus SPPO" as modo,
            indicadores,
            tecnologia,
            status,
            versao
        from {{ ref("sppo_veiculo_dia") }}
        where data = date("{{ var('run_date') }}")
    )
select *
from sppo
