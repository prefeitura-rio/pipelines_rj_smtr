/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/

{{
  config(
    materialized='ephemeral'
  )
}}

select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v1") }}
where feed_start_date < date("{{ var('DATA_GTFS_V4_INICIO') }}")
union all by name
select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v2") }}
where feed_start_date >= date("{{ var('DATA_GTFS_V4_INICIO') }}")