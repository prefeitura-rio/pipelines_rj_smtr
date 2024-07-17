{{
  config(
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": "data",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}
{% set transacao_table = ref('transacao') %}
{% if execute %}
  {% if is_incremental() %}

    {% set partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
    {% endset %}

    {{ log("Running query: \n"~partitions_query, info=True) }}
    {% set partitions = run_query(partitions_query) %}

    {% set partition_list = partitions.columns[0].values() %}
    {{ log("trasacao partitions: \n"~partition_list, info=True) }}
  {% endif %}
{% endif %}

WITH transacao AS (
  SELECT
    t.data,
    t.hora,
    t.datetime_transacao,
    t.datetime_processamento,
    t.datetime_captura,
    t.modo,
    t.id_consorcio,
    t.consorcio,
    t.id_operadora,
    t.operadora,
    t.id_servico_jae,
    t.servico_jae,
    t.descricao_servico_jae,
    t.id_transacao,
    t.longitude,
    t.latitude,
    IFNULL(t.longitude, 0) AS longitude_tratada,
    IFNULL(t.latitude, 0) AS latitude_tratada,
    s.longitude AS longitude_servico,
    s.latitude AS latitude_servico,
    s.id_servico_gtfs,
    s.id_servico_jae AS id_servico_jae_cadastro
  FROM
    {{ ref("transacao") }} t
  LEFT JOIN
    {{ ref("servicos") }} s
  ON
    t.id_servico_jae = s.id_servico_jae
    AND t.data >= s.inicio_vigencia AND (t.data <= s.fim_vigencia OR s.fim_vigencia IS NULL)
  WHERE
    {% if is_incremental() %}
      {% if partition_list|length > 0 %}
        data IN ({{ partition_list|join(', ') }})
      {% else %}
        data = "2000-01-01"
      {% endif %}
    {% endif %}
),
indicadores AS (
  SELECT
    * EXCEPT(id_servico_gtfs, latitude_tratada, longitude_tratada, id_servico_jae_cadastro),
    latitude_tratada = 0 OR longitude_tratada = 0 AS indicador_geolocalizacao_zerada,
    (
      (latitude_tratada != 0 OR longitude_tratada != 0)
      AND NOT ST_INTERSECTSBOX(ST_GEOGPOINT(longitude_tratada, latitude_tratada), -43.87, -23.13, -43.0, -22.59)
    ) AS indicador_geolocalizacao_fora_rio,
    (
      latitude_tratada != 0
      AND longitude_tratada != 0
      AND latitude_servico IS NOT NULL
      AND longitude_servico IS NOT NULL
      AND modo = "BRT"
      AND ST_DISTANCE(ST_GEOGPOINT(longitude_tratada, latitude_tratada), ST_GEOGPOINT(longitude_servico, latitude_servico)) > 100
    ) AS indicador_geolocalizacao_fora_stop,
    id_servico_gtfs IS NULL AND id_servico_jae_cadastro IS NOT NULL AND modo IN ("Ônibus", "BRT") AS indicador_servico_fora_gtfs,
    id_servico_jae_cadastro IS NULL AS indicador_servico_fora_vigencia
  FROM
    transacao
)
SELECT
  * EXCEPT(indicador_servico_fora_gtfs, indicador_servico_fora_vigencia),
  CASE
    WHEN indicador_geolocalizacao_zerada = TRUE THEN "Geolocalização zerada"
    WHEN indicador_geolocalizacao_fora_rio = TRUE THEN "Geolocalização fora do município"
    WHEN indicador_geolocalizacao_fora_stop = TRUE THEN "Geolocalização fora do stop"
  END AS descricao_geolocalizacao_invalida,
  indicador_servico_fora_gtfs,
  indicador_servico_fora_vigencia,
  '{{ var("version") }}' as versao
FROM
  indicadores
WHERE
  indicador_geolocalizacao_zerada = TRUE
  OR indicador_geolocalizacao_fora_rio = TRUE
  OR indicador_geolocalizacao_fora_stop = TRUE
  OR indicador_servico_fora_gtfs = TRUE
  OR indicador_servico_fora_vigencia = TRUE