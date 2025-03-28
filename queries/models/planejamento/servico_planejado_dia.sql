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
    ),
    servico_planejado_dia as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            max(case when sentido = 'I' then extensao else 0 end) as extensao_ida,
            max(case when sentido = 'V' then extensao else 0 end) as extensao_volta,
            sum(case when sentido = 'I' then partidas else 0 end) as partidas_ida,
            sum(case when sentido = 'V' then partidas else 0 end) as partidas_volta,
            sum(partidas) as partidas,
            sum(quilometragem) / 2 as quilometragem,  -- TODO: sum(quilometragem) as quilometragem
            case
                when min(extensao) = 0 then sum(partidas) else sum(partidas) / 2
            end as viagens,
            modo
        from servico_planejado_faixa_horaria
        group by
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            modo
    )
select *
from servico_planejado_dia
