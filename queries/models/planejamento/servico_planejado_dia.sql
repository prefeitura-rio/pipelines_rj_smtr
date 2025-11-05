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

{# {% set calendario = ref("calendario") %} #}
{% set calendario = "rj-smtr.planejamento.calendario" %}
{% if execute %}
    {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{% endif %}

with
    os as (
        select *
        from {{ ref("aux_ordem_servico_faixa_horaria") }}
        {# from `rj-smtr.planejamento_staging.aux_ordem_servico_faixa_horaria` #}
        {# from {{ ref("ordem_servico_faixa_horaria") }} #}
        {# from `rj-smtr.planejamento.ordem_servico_faixa_horaria` #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    servico_planejado_faixa_horaria as (
        select *
        from {{ ref("servico_planejado_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    servico_planejado_agg as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            sum(quilometragem) / 2 as quilometragem,  -- TODO: sum(quilometragem) as quilometragem,
        from servico_planejado_faixa_horaria
        group by data, feed_version, feed_start_date, tipo_dia, tipo_os, servico
    ),
    os_dia as (
        select distinct
            c.data,
            o.feed_version,
            o.feed_start_date,
            o.tipo_dia,
            o.tipo_os,
            o.servico,
            o.consorcio,
            o.extensao_ida,
            o.extensao_volta,
            o.partidas_ida_dia,
            o.partidas_volta_dia,
            o.viagens_dia,
            "Ônibus SPPO" as modo
        from {{ calendario }} c
        join os o using (feed_version, feed_start_date, tipo_dia, tipo_os)
        where {{ incremental_filter }}
    ),
    final as (
        select
            o.data,
            o.feed_version,
            o.feed_start_date,
            o.tipo_dia,
            o.tipo_os,
            servico,
            consorcio,
            extensao_ida,
            extensao_volta,
            partidas_ida_dia,
            partidas_volta_dia,
            s.quilometragem,
            viagens_dia,
            "Ônibus SPPO" as modo
        from os_dia o
        left join servico_planejado_agg s using (data, servico)
    )
select *
from final
