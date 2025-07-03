{% snapshot snapshot_tecnologia_servico %}

    {{
        config(
            target_schema="planejamento_staging",
            unique_key="concat(servico, '-', inicio_vigencia)",
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
    from {{ ref("tecnologia_servico") }}

{% endsnapshot %}
