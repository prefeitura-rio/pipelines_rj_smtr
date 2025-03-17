-- depends_on: {{ ref('ordem_servico_trajeto_alternativo_gtfs') }}
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
        {% set evento_query %}
            select distinct evento
            from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endset %}
    {% else %}
        {% set evento_query %}
            select distinct evento
            from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
        {% endset %}
    {% endif -%}
    {%- set eventos_trajetos_alternativos = (
        run_query(evento_query).columns[0].values()
    ) -%}
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
    ),
    trip_info as (
        select
            feed_start_date,
            feed_version,
            trip_short_name as servico,
            route_id,
            shape_id,
            case
                when direction_id = '0' then 'I' when direction_id = '1' then 'V'
            end as sentido,
            case
                when
                    (
                        {% for evento in eventos_trajetos_alternativos %}
                            trip_headsign like "%{{evento}}%" or
                        {% endfor %}
                        service_id = "EXCEP"
                    )
                then true
                else false
            end as indicador_trajeto_alternativo
        from {{ ref("trips_gtfs") }} t
        where
            {% if is_incremental() %}
                feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% endif %}
        qualify
            row_number() over (
                partition by
                    feed_start_date, feed_version, trip_short_name, direction_id
                order by trip_id
            )
            = 1
    )
select s.*, t.route_id, t.shape_id, t.indicador_trajeto_alternativo, t.sentido
from os_sppo s
left join
    trip_info t
    on s.feed_start_date = t.feed_start_date
    and s.feed_version = t.feed_version
    and s.servico = t.servico
    and s.sentido = t.sentido
