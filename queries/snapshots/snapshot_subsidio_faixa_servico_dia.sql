{% snapshot snapshot_subsidio_faixa_servico_dia %}

    {{
        config(
            target_schema="financeiro_staging",
            unique_key="concat(data, '-', faixa_horaria_inicio, '-', servico)",
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
    from {{ ref("subsidio_faixa_servico_dia") }}

{% endsnapshot %}
