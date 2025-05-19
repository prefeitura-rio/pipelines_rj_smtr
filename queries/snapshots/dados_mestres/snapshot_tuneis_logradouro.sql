{% snapshot snapshot_tuneis_logradouro %}

    {{
        config(
            target_schema="dados_mestres",
            unique_key="id_trecho",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
        )
    }}

    select
        *,
        timestamp(
            current_datetime("America/Sao_Paulo"), "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ source("dados_mestres", "logradouro") }}
    where tipo = "Túnel"

{% endsnapshot %}

{# partition_by={"field": "data", "data_type": "date", "granularity": "day"}, #}

