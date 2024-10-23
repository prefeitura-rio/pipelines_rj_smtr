{{
    config(
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{# {% set gtfs_feed_info = ref("feed_info_gtfs") %} #}
{% set gtfs_feed_info = "rj-smtr.gtfs.feed_info" %}

{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_feeds_query %}
            select concat("'", feed_start_date, "'")
            from {{ gtfs_feed_info }}
            where
                feed_start_date <= date("{{ var('date_range_end') }}")
                and (feed_end_date IS NULL OR feed_end_date >= date("{{ var('date_range_end') }}"))
        {% endset %}

        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    calendar as (
        select *
        from `rj-smtr.gtfs.calendar`
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    calendar_dates as (
        select
            date as data,
            case
                when exception_type = '2'
                then
                    regexp_replace(
                        service_id,
                        "^[U|S|D]_",
                        case
                            when extract(dayofweek from date) = 7
                            then "S_"
                            when extract(dayofweek from date) = 1
                            then "D_"
                            else "U_"
                        end
                    )
                else service_id
            end as service_id,
            exception_type,
            feed_start_date,
        from `rj-smtr.gtfs.calendar_dates`
        where
            feed_start_date in ({{ gtfs_feeds | join(", ") }})
            and date between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
    ),
    datas as (
        select distinct data, extract(dayofweek from data) as dia_semana,
        from
            unnest(
                generate_date_array(
                    date("{{ var('date_range_start') }}"),
                    date("{{ var('date_range_end') }}")
                )
            ) as data
    ),
    datas_service as (
        select d.data, c.service_id, c.feed_version, c.feed_start_date
        from datas d
        join
            calendar c
            on d.data >= c.feed_start_date
            and (d.data <= feed_end_date or feed_end_date is null)
            and (
                (d.dia_semana = 1 and c.sunday = '1')
                or (d.dia_semana = 2 and c.monday = '1')
                or (d.dia_semana = 3 and c.tuesday = '1')
                or (d.dia_semana = 4 and c.wednesday = '1')
                or (d.dia_semana = 5 and c.thursday = '1')
                or (d.dia_semana = 6 and c.friday = '1')
                or (d.dia_semana = 7 and c.saturday = '1')
            )
    ),
    service_ids_retirados as (
        select d.*, c.service_id as c_service_id,
        from datas_calendar d
        left join
            (select * from calendar_dates where exception_type = '2') c using (
                data, service_id
            )
        where c.service_id is null

    ),
    service_ids_adicionados as (
        select data, service_id, dia_semana, feed_start_date
        from service_ids_retirados

        union distinct

        select data, service_id, dia_semana, feed_start_date
        from calendar_dates
        where exception_type = '1'
    ),
    service_id_agg as (
        select data, feed_start_date, array_agg(service_id) as service_ids
        from service_ids_adicionados
        group by 1, 2, 3
    ),
    calendario_gtfs as (
        select
            data,
            case
                when "D_REG" in unnest(service_ids)
                then "Domingo"
                when "S_REG" in unnest(service_ids)
                then "Sábado"
                when "U_REG" in unnest(service_ids)
                then "Dia Útil"
            end as tipo_dia,
            "Regular" as tipo_os,
            service_ids,
            feed_start_date
        from service_id_agg
    ),
    modificacoes_manuais as (
        select
            c.data,
            coalesce(m.tipo_dia, c.tipo_dia) as tipo_dia,
            coalesce(m.tipo_os, c.tipo_os) as tipo_os,
            service_ids,
            coalesce(m.feed_start_date, c.feed_start_date) as feed_start_date
        from calendario_gtfs c
        left join {{ ref("aux_calendario_manual") }} m using (data)
    )
select
    data,
    tipo_dia,
    case
        when c.tipo_os = "Extraordinária - Verão"
        then "Verão"
        when c.tipo_os like "%Madonna%"
        then "Madonna"
        when c.tipo_os = "Regular"
        then null
        else c.tipo_os
    end as subtipo_dia,
    c.tipo_os,
    c.service_ids,
    i.feed_version
    feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from modificacoes_manuais c
join `rj-smtr.gtfs.feed_info` i using (feed_start_date)
