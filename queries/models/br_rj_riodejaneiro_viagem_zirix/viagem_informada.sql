{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite"
  )
}}

SELECT
  data_viagem AS data,
  datetime_partida,
  datetime_chegada,
  datetime_processamento,
  id_veiculo,
  trip_id,
  route_id,
  shape_id,
  servico,
  CASE
    WHEN sentido = 'I' THEN 'Ida'
    WHEN sentido = 'V' THEN 'Volta'
    ELSE sentido
  END AS sentido,
FROM
  {{ ref("staging_viagem_informada") }}
{% if is_incremental() %}
  WHERE
    data BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endif %}