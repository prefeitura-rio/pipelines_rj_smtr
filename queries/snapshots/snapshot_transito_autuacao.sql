{% snapshot snapshot_transito_autuacao %}

    {{
        config(
            target_schema="transito_staging",
            unique_key="id_autuacao",
            strategy="timestamp",
            updated_at="datetime_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    select
        * except (datetime_ultima_atualizacao),
        safe_cast(
            datetime_ultima_atualizacao as timestamp
        ) as datetime_ultima_atualizacao
    from {{ ref("autuacao") }}

{% endsnapshot %}
