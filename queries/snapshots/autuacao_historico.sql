{% snapshot autuacao_historico %}

    {{
        config(
            target_schema="transito",
            unique_key="id_autuacao",
            strategy="timestamp",
            updated_at="datetime_ultima_atualizacao",
            invalidate_hard_deletes=True,
        )
    }}

    select
        * except (datetime_ultima_atualizacao),
        safe_cast(
            datetime_ultima_atualizacao as timestamp
        ) as datetime_ultima_atualizacao
    from {{ ref("autuacao") }}

{% endsnapshot %}
