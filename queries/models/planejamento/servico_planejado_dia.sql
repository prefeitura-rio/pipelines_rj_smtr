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

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
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
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            sentido,
            vista,
            consorcio,
            "Ônibus SPPO" as modo,
            horario_inicio,
            horario_fim,
            extensao,
            sum(partidas) as partidas,
            sum(quilometragem) as quilometragem
        from {{ ref("aux_os_sppo_sentido_dia") }}
        {% if is_incremental() %}
            where
                {{ incremental_filter }}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
        group by
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            modo,
            sentido,
            extensao,
    )
select *
from os_sppo
