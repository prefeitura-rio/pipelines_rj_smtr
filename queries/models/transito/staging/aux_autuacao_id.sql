{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key="id_auto_infracao",
        incremental_strategy="insert_overwrite",
    )
}}

select
    s.data,
    to_hex(sha256(concat(generate_uuid(), id_auto_infracao))) as id_autuacao,
    id_auto_infracao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    (
        select distinct id_auto_infracao, data
        from {{ ref("autuacao_citran") }}
        union all
        select distinct id_auto_infracao, data
        from {{ ref("autuacao_serpro") }}
    ) s
{% if is_incremental() %}
    left join {{ this }} as t using (data, id_auto_infracao)
    where
        t.id_auto_infracao is null
        and s.data between date("{{var('date_range_start')}}") and date(
            "{{var('date_range_end')}}"
        )
{% endif %}
