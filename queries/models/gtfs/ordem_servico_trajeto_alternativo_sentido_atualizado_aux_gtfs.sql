/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/
{{ config(materialized="ephemeral") }}

select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v1") }}
where
    feed_start_date < date("{{ var('DATA_GTFS_V4_INICIO') }}")
--fmt:off
full outer union all by name
--fmt:on
select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v2") }}
where
    feed_start_date between date("{{ var('DATA_GTFS_V4_INICIO') }}") and date(
        "{{ var('DATA_GTFS_V5_INICIO') }}"
    )
--fmt:off
full outer union all by name
--fmt:on
select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v3") }}
where feed_start_date > date("{{ var('DATA_GTFS_V5_INICIO') }}")
