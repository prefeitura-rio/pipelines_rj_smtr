{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

select
    data_particao as data,
    horario as hora,
    id_estacao,
    temperatura,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ source("clima_estacao_meteorologica", "meteorologia_inmet") }}
where
    data_particao >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    {% if is_incremental() %}
        and data_particao between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}
