{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

with
    servico_planejado_faixa_horaria as (
        select *
        from {{ ref("servico_planejado_faixa_horaria") }}
        where {{ incremental_filter }}
    )
