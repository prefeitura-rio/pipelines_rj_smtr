{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{# {% set calendario = ref("calendario") %} #}
{% set calendario = "rj-smtr.planejamento.calendario" %}
{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
        {% endset %}
        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    os_sppo as (
        select *, "Ônibus SPPO" as modo
        from {{ ref("aux_os_sppo_faixa_horaria_dia") }}
        {% if is_incremental() %}
            where
                {{ incremental_filter }}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    )
select *
from os_sppo
