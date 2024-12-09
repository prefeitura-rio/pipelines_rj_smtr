{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    viagem_valida as (
        select
            v.*,
            c.tipo_os,
            case when sentido = 'Circular' then 'Ida' else sentido end as sentido_join
        {# from {{ ref("viagem_validacao") }} #}
        from `rj-smtr.monitoramento.viagem_validacao` v
        join {{ ref("calendario") }} c using (data)
        where
            indicador_viagem_valida
            {% if is_incremental() %}
                and data between date({{ var("date_range_start") }}) and date(
                    {{ var("date_range_end") }}
                )
            {% endif %}
    ),
    servico_planejado as (
        select *
        from {{ ref("servico_planejado") }}
        {% if is_incremental() %}
            where
                data between date({{ var("date_range_start") }}) and date(
                    {{ var("date_range_end") }}
                )
        {% endif %}
    )
