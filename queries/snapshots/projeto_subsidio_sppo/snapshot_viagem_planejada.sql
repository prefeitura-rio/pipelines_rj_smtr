{% snapshot snapshot_viagem_planejada %}

    {{
        config(
            target_schema="projeto_subsidio_sppo_staging",
            unique_key="concat(data, '-', servico, '-', sentido, '-', faixa_horaria_inicio, '-', ifnull(trip_id, ''), '-', ifnull(trip_id_planejado, ''), '-', ifnull(shape_id, ''), '-', ifnull(shape_id_planejado, ''))",
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
    from {{ ref("viagem_planejada") }}
    where distancia_total_planejada != 0
{% endsnapshot %}
