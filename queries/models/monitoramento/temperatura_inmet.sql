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

select data_particao as data, horario as hora, id_estacao, temperatura
from {{ source("clima_estacao_meteorologica", "meteorologia_inmet") }}
