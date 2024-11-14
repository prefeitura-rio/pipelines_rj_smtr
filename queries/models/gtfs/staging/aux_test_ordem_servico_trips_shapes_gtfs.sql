SELECT
  DISTINCT feed_start_date,
  tipo_os,
  tipo_dia,
  servico,
  faixa_horaria_inicio
FROM
  {{ ref("ordem_servico_trips_shapes_gtfs") }}
WHERE
  feed_start_date = '{{ var('data_versao_gtfs') }}'