{% snapshot snapshot_viagem_completa %}

    {{
        config(
            target_schema="projeto_subsidio_sppo",
            unique_key="id_viagem",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    select
        * except (versao_modelo),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("viagem_completa") }}
    qualify row_number() over (partition by id_viagem) = 1

{% endsnapshot %}
