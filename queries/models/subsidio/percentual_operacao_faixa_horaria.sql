{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
select *
from {{ ref("percentual_operacao_faixa_horaria_v2") }}
where data > date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
full outer union all by name
select *
from {{ ref("percentual_operacao_faixa_horaria_v1") }}
where data <= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
