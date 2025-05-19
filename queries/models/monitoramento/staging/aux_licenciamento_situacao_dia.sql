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

select data, id_veiculo, placa, ultima_situacao as situacao
from {{ ref("aux_licenciamento") }}
where
    data between date("{{ var('date_range_start') }}") and date(
        "{{ var('date_range_end') }}"
    )
