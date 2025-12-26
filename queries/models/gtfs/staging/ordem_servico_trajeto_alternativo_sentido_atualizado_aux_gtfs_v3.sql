/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/
{{ config(materialized="ephemeral") }}

-- 1. Busca anexo de trajetos alternativos
select * except (sentido), left(sentido, 1) as sentido
from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
where feed_start_date = date("{{ var('data_versao_gtfs') }}")
