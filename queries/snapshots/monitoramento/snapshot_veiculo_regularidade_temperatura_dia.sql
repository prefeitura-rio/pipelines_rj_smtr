{% snapshot snapshot_veiculo_regularidade_temperatura_dia %}

    {{
        config(
            target_schema="monitoramento_staging",
            unique_key="concat(data, '-', id_veiculo)",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    select
        *,
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("veiculo_regularidade_temperatura_dia") }}

{% endsnapshot %}
