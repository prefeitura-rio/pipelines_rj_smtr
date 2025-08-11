{{
    config(
        materilized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

with
    integracao_calculada as (
        select
            *,
            string_agg(id_transacao, '_') over (
                partition by id_integracao order by sequencia_integracao
            ) as id_join
        from {{ ref("aux_integracao_calculada") }}
    ),
    integracao_jae as (
        select
            id_integracao,
            string_agg(id_transacao, '_') over (
                partition by id_integracao order by sequencia_integracao
            ) as id_join
        from {{ ref("integracao") }}
        where data between '2025-08-06' and '2025-08-08'
    )
select c.*
from integracao_calculada c
left join integracao_jae j using (id_join)
where j.id_integracao is null
