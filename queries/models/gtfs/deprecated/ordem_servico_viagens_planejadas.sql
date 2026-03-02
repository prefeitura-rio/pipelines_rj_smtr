with
    data_versao as (
        select
            feed_start_date,
            feed_start_date as data_inicio,
            coalesce(
                date_sub(
                    lead(feed_start_date) over (order by feed_start_date),
                    interval 1 day
                ),
                last_day(feed_start_date, month)
            ) as data_fim
        from (select distinct feed_start_date, from {{ ref("ordem_servico_gtfs") }})
    ),
    subsidio_data_versao_efetiva as (
        select * except (tipo_dia), split(tipo_dia, " - ")[0] as tipo_dia
        from {{ ref("subsidio_data_versao_efetiva") }}
    )
select data, sd.tipo_dia, servico, viagens_planejadas
from
    unnest(
        generate_date_array(
            (select min(data_inicio) from data_versao),
            (select max(data_fim) from data_versao)
        )
    ) as data
left join data_versao as d on data between d.data_inicio and d.data_fim
left join
    subsidio_data_versao_efetiva as sd
    on data = sd.data
    and (d.feed_start_date = sd.feed_start_date or sd.feed_start_date is null)
left join
    {{ ref("ordem_servico_gtfs") }} as o
    on d.feed_start_date = o.feed_start_date
    and sd.tipo_dia = o.tipo_dia
