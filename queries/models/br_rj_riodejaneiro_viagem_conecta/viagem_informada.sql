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

{% set incremental_filter %}
  DATE(data) = DATE("{{var('run_date')}}")
{% endset %}

{% set staging_viagem_informada = ref('staging_viagem_informada') %}
{% if execute %}
  {% if is_incremental() %}
    {% set partitions_query %}
      SELECT DISTINCT
        CONCAT("'", DATE(data_viagem), "'") AS data_viagem
      FROM
        {{ staging_viagem_informada }}
      WHERE
        {{ incremental_filter }}
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}
  {% endif %}
{% endif %}

WITH staging_data AS (
  SELECT
    data_viagem AS data,
    datetime_partida,
    datetime_chegada,
    datetime_processamento,
    timestamp_captura AS datetime_captura,
    id_veiculo,
    trip_id,
    route_id,
    shape_id,
    servico,
    CASE
      WHEN sentido = 'IDA' THEN 'Ida'
      WHEN sentido = 'VOLTA' THEN 'Volta'
      ELSE sentido
    END AS sentido,
    id_viagem
  FROM
    {{ staging_viagem_informada }}
  {% if is_incremental() %}
    WHERE
      {{ incremental_filter }}
  {% endif %}
),
complete_partitions AS (
  SELECT
    *,
    0 AS priority
  FROM
    staging_data

  {% if is_incremental() and partitions|length > 0 %}
    UNION ALL

    SELECT
      * EXCEPT(versao, datetime_ultima_atualizacao),
      1 AS priority
    FROM
      {{ this }}
    WHERE
      data IN ({{ partitions|join(', ') }})
  {% endif %}
),
deduplicado AS (
  SELECT
    * EXCEPT(rn, priority)
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id_viagem ORDER BY datetime_captura DESC, priority) AS rn
      FROM
        complete_partitions
    )
  WHERE
    rn = 1
)
SELECT
  *,
  '{{ var("version") }}' AS versao,
  CURRENT_DATETIME("America/Sao_Paulo") as datetime_ultima_atualizacao
FROM
  deduplicado