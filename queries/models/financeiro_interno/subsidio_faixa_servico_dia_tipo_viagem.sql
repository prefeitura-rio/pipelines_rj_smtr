{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    subsidio_faixa_servico_dia_tipo_viagem as (
        select *
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem_v3") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V22_INICIO') }}")
        -- fmt: off
        full outer union all by name
        -- fmt: on
        select *, "Ônibus SPPO" as modo
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem_v2") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V22_INICIO') }}")
        --fmt:off
        full outer union all by name
        --fmt:on
        select *, "Ônibus SPPO" as modo
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem_v1") }}
        where data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    )
select *, '{{ invocation_id }}' as id_execucao_dbt
from subsidio_faixa_servico_dia_tipo_viagem
