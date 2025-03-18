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
        from {{ ref("aux_os_sppo_faixa_horaria_sentido_dia") }}
        {% if is_incremental() %}
            where
                {{ incremental_filter }}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    trajetos_alternativos as (
        select
            feed_start_date,
            feed_version,
            trip_id,
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
        from {{ ref("trips_gtfs") }}
        where
            {% if is_incremental() %}
                feed_start_date in ({{ gtfs_feeds | join(", ") }}) and
            {% endif %}
            service_id not like "%_DESAT_%"
    ),
    trip_info as (
        select
            ta.feed_start_date,
            ta.feed_version,
            ta.servico,
            ta.trip_id,
            ta.route_id,
            ta.shape_id,
            ta.sentido,
            ta.indicador_trajeto_alternativo
        from trajetos_alternativos ta
        left join
            {{ ref("shapes_geom_gtfs") }} s using (
                feed_start_date, feed_version, shape_id
            )
        qualify
            row_number() over (
                partition by
                    ta.feed_start_date,
                    ta.feed_version,
                    ta.trip_short_name,
                    ta.sentido,
                    case
                        when ta.indicador_trajeto_alternativo then ta.shape_id else null
                    end
                order by shape_distance desc
            )
            = 1
    ),
select s.*, t.trip_id, t.route_id, t.shape_id, t.indicador_trajeto_alternativo
from os_sppo s
left join
    trip_info t
    on s.feed_start_date = t.feed_start_date
    and s.feed_version = t.feed_version
    and s.servico = t.servico
    and s.sentido = t.sentido
