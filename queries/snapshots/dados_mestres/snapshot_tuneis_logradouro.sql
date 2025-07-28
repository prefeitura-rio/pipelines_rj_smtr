{% snapshot snapshot_tuneis_logradouro %}

    {{
        config(
            target_schema="planejamento_staging",
            unique_key="id_trecho",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
        )
    }}

    select
        *,
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ source("dados_mestres", "logradouro") }}
    where tipo = "Túnel"

{% endsnapshot %}
