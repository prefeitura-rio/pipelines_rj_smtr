{{
    config(
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="viagem_planejada",
    )
}}
