{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="ordem_servico_trips_shapes",
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
          ((o.tipo_dia = t.tipo_dia AND o.tipo_os NOT IN ("CNU", "Enem", "Eleição"))
          OR (o.tipo_dia = "Ponto Facultativo" AND t.tipo_dia = "Dia Útil" AND o.tipo_os != "CNU")
          OR (o.feed_start_date = "2024-08-16" AND o.tipo_os = "CNU" AND o.tipo_dia = "Domingo" AND t.tipo_dia = "Sabado") -- Domingo CNU
          OR (o.feed_start_date IN ("2024-09-29", "2024-11-06") AND o.tipo_os IN ("Enem", "Eleição") AND o.tipo_dia = "Domingo" AND t.tipo_dia = "Sabado")) -- Domingo Enem/Eleição
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
select
    feed_version,
    feed_start_date,
    o.feed_end_date,
    tipo_os,
    tipo_dia,
    servico,
    o.vista,
    o.consorcio,
    sentido,
    case
        when feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
        then fh.partidas
        else null
    end as partidas_total_planejada,
    distancia_planejada,
    case
        when feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
        then fh.quilometragem
        else distancia_total_planejada
    end as distancia_total_planejada,
    inicio_periodo,
    fim_periodo,
    case
        when feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
        then fh.faixa_horaria_inicio
        else "00:00:00"
    end as faixa_horaria_inicio,
    case
        when feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
        then fh.faixa_horaria_fim
        else "23:59:59"
    end as faixa_horaria_fim,
    trip_id_planejado,
    trip_id,
    shape_id,
    shape_id_planejado,
    shape,
    case
        when sentido = "C" and split(shape_id, "_")[offset(1)] = "0"
        then "I"
        when sentido = "C" and split(shape_id, "_")[offset(1)] = "1"
        then "V"
        when sentido = "I" or sentido = "V"
        then sentido
    end as sentido_shape,
    s.start_pt,
    s.end_pt,
    id_tipo_trajeto,
from ordem_servico_trips as o
left join shapes as s using (feed_version, feed_start_date, shape_id)
left join
    {{ ref("ordem_servico_faixa_horaria") }} as fh
    -- rj-smtr-dev.gtfs.ordem_servico_faixa_horaria AS fh
    using (feed_version, feed_start_date, tipo_os, tipo_dia, servico)
where
    {% if is_incremental() -%}
        feed_start_date = '{{ var("data_versao_gtfs") }}' and
    {% endif -%}
    (
        (
            feed_start_date >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
            and (fh.quilometragem != 0 and (fh.partidas != 0 or fh.partidas is null))
        )
        or feed_start_date < '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
    )
