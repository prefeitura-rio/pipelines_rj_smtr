{{
  config(
    partition_by = {
      "field": "feed_start_date",
      "data_type": "date",
      "granularity": "day"
    },
    alias = "ordem_servico_trips_shapes"
  )
}}

WITH
  -- 1. Busca os shapes em formato geográfico
  shapes AS (
    SELECT
      *
    FROM
      {{ ref("shapes_geom_gtfs") }}
    {% if is_incremental() -%}
    WHERE
      feed_start_date = '{{ var("data_versao_gtfs") }}'
    {% endif -%}
  ),
  -- 2. Trata a OS, inclui trip_ids e ajusta nomes das colunas
  ordem_servico_tratada AS (
  SELECT
    *
  FROM
    (
      (
      SELECT
        o.feed_version,
        o.feed_start_date,
        o.feed_end_date,
        o.tipo_os,
        o.tipo_dia,
        servico,
        vista,
        consorcio,
        sentido,
        distancia_planejada,
        distancia_total_planejada,
        inicio_periodo,
        fim_periodo,
        trip_id,
        shape_id,
        indicador_trajeto_alternativo
      FROM
        {{ ref("ordem_servico_sentido_atualizado_aux_gtfs") }} AS o
      LEFT JOIN
        {{ ref("trips_filtrada_aux_gtfs") }} AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND
          ((o.tipo_dia = t.tipo_dia AND o.tipo_os NOT IN ("CNU", "Enem"))
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil" AND o.tipo_os != "CNU")
          OR (o.feed_start_date = "2024-08-16" AND o.tipo_os = "CNU" AND o.tipo_dia = "Domingo" AND t.tipo_dia = "Sabado") -- Domingo CNU
          OR (o.feed_start_date IN ("2024-09-29", "2024-11-06") AND o.tipo_os = "Enem" AND o.tipo_dia = "Domingo" AND t.tipo_dia = "Sabado")) -- Domingo Enem
        AND
          ((o.sentido IN ("I", "C") AND t.direction_id = "0")
          OR (o.sentido = "V" AND t.direction_id = "1"))
      WHERE
        indicador_trajeto_alternativo IS FALSE
      )
    UNION ALL
      (
      SELECT
        o.feed_version,
        o.feed_start_date,
        o.feed_end_date,
        o.tipo_os,
        o.tipo_dia,
        servico,
        o.vista || " " || ot.evento AS vista,
        o.consorcio,
        sentido,
        ot.distancia_planejada,
        distancia_total_planejada,
        COALESCE(ot.inicio_periodo, o.inicio_periodo) AS inicio_periodo,
        COALESCE(ot.fim_periodo, o.fim_periodo) AS fim_periodo,
        trip_id,
        shape_id,
        indicador_trajeto_alternativo
      FROM
        {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs") }} AS ot
      LEFT JOIN
        {{ ref("ordem_servico_sentido_atualizado_aux_gtfs") }} AS o
      USING
        (feed_version,
          tipo_os,
          servico,
          sentido)
      LEFT JOIN
        {{ ref("trips_filtrada_aux_gtfs") }} AS t
      ON
        t.feed_version = o.feed_version
        AND o.servico = t.trip_short_name
        AND
          (o.tipo_dia = t.tipo_dia
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil")
          OR (t.tipo_dia = "EXCEP")) -- Inclui trips do service_id/tipo_dia "EXCEP"
        AND
          ((o.sentido IN ("I", "C") AND t.direction_id = "0")
          OR (o.sentido = "V" AND t.direction_id = "1"))
        AND t.trip_headsign LIKE CONCAT("%", ot.evento, "%")
      WHERE
        indicador_trajeto_alternativo IS TRUE
        AND trip_id IS NOT NULL -- Remove serviços de tipo_dia sem planejamento
      )
    )
  ),
  -- 3. Inclui trip_ids de ida e volta para trajetos circulares, ajusta shape_id para trajetos circulares e inclui id_tipo_trajeto
  ordem_servico_trips AS (
    SELECT
      * EXCEPT(shape_id, indicador_trajeto_alternativo),
      shape_id AS shape_id_planejado,
      CASE
        WHEN sentido = "C" THEN shape_id || "_" || SPLIT(trip_id, "_")[OFFSET(1)]
      ELSE
      shape_id
    END
      AS shape_id,
      CASE
        WHEN indicador_trajeto_alternativo IS FALSE THEN 0 -- Trajeto regular
        WHEN indicador_trajeto_alternativo IS TRUE THEN 1 -- Trajeto alternativo
    END
      AS id_tipo_trajeto,
    FROM
    (
      (
        SELECT
          DISTINCT * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          trip_id
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "I"
          OR sentido = "V"
      )
      UNION ALL
      (
        SELECT
          * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          CONCAT(trip_id, "_0") AS trip_id,
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "C"
      )
      UNION ALL
      (
        SELECT
          * EXCEPT(trip_id),
          trip_id AS trip_id_planejado,
          CONCAT(trip_id, "_1") AS trip_id,
        FROM
          ordem_servico_tratada
        WHERE
          sentido = "C"
      )
    )
  )
SELECT
  feed_version,
  feed_start_date,
  o.feed_end_date,
  tipo_os,
  tipo_dia,
  servico,
  vista,
  o.consorcio,
  sentido,
  CASE
    WHEN feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}' THEN fh.partidas
    ELSE NULL
  END AS partidas_total_planejada,
  distancia_planejada,
  CASE
    WHEN feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}' THEN fh.quilometragem
    ELSE distancia_total_planejada
  END AS distancia_total_planejada,
  inicio_periodo,
  fim_periodo,
  CASE
    WHEN feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}' THEN fh.faixa_horaria_inicio
    ELSE "00:00:00"
  END AS faixa_horaria_inicio,
  CASE
    WHEN feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}' THEN fh.faixa_horaria_fim
    ELSE "23:59:59"
  END AS faixa_horaria_fim,
  trip_id_planejado,
  trip_id,
  shape_id,
  shape_id_planejado,
  shape,
  CASE
    WHEN sentido = "C" AND SPLIT(shape_id, "_")[OFFSET(1)] = "0" THEN "I"
    WHEN sentido = "C" AND SPLIT(shape_id, "_")[OFFSET(1)] = "1" THEN "V"
    WHEN sentido = "I" OR sentido = "V" THEN sentido
END
  AS sentido_shape,
  s.start_pt,
  s.end_pt,
  id_tipo_trajeto,
FROM
  ordem_servico_trips AS o
LEFT JOIN
  shapes AS s
USING
  (feed_version,
    feed_start_date,
    shape_id)
LEFT JOIN
  {{ ref("ordem_servico_faixa_horaria") }} AS fh
  -- rj-smtr-dev.gtfs.ordem_servico_faixa_horaria AS fh
USING
  (feed_version, feed_start_date, tipo_os, tipo_dia, servico)
WHERE
{% if is_incremental() -%}
  feed_start_date = '{{ var("data_versao_gtfs") }}' AND
{% endif -%}
  (
    (
    feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
    AND
      (
        fh.quilometragem != 0
        AND (fh.partidas != 0 OR fh.partidas IS NULL)
      )
    )
    OR
      feed_start_date < '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
  )