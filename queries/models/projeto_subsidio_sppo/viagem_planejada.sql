{{ config(
    materialized='incremental',
        partition_by={
        "field":"data",
        "data_type": "date",
        "granularity":"day"
    },
    unique_key=['data', 'trip_id'],
    incremental_strategy='insert_overwrite'
)
}}

{% if var("run_date") <= var("DATA_SUBSIDIO_V6_INICIO") %}

-- 1. Define datas do período planejado
with data_efetiva as (
    select
        data,
        tipo_dia,
        data_versao_shapes,
        data_versao_trips,
        data_versao_frequencies
    from {{ ref("subsidio_data_versao_efetiva") }}
    where data between date_sub("{{ var("run_date") }}", interval 1 day) and date("{{ var("run_date") }}")
),
-- 2. Puxa dados de distancia quadro no quadro horário
quadro as (
    select
        e.data,
        e.tipo_dia,
        p.* except(tipo_dia, data_versao, horario_inicio, horario_fim),
        IF(horario_inicio IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_inicio, ":")) = 3,
            DATETIME_ADD(
                DATETIME(
                    e.data,
                    PARSE_TIME("%T",
                        CONCAT(
                        SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(1)] AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(2)] AS INT64)
                        )
                    )
                ),
                INTERVAL DIV(SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) DAY
            ),
            NULL
        ) AS inicio_periodo,
        IF(horario_fim IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_fim, ":")) = 3,
            DATETIME_ADD(
                DATETIME(
                    e.data,
                    PARSE_TIME("%T",
                        CONCAT(
                        SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(1)] AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(2)] AS INT64)
                        )
                    )
                ),
                INTERVAL DIV(SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) DAY
            ),
            NULL
        ) AS fim_periodo
    from
        data_efetiva e
    inner join (
        select *
        from {{ ref("subsidio_quadro_horario") }}
        {% if is_incremental() %}
        where
            data_versao in (select data_versao_frequencies from data_efetiva)
        {% endif %}
    ) p
    on
        e.data_versao_frequencies = p.data_versao
    and
        e.tipo_dia = p.tipo_dia
),
-- 3. Trata informação de trips: adiciona ao sentido da trip o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
trips as (
    select
        e.data,
        t.*
    from (
        select *
        from {{ ref('subsidio_trips_desaninhada') }}
        {% if is_incremental() %}
        where
            data_versao in (select data_versao_trips from data_efetiva)
        {% endif %}
    ) t
    inner join
        data_efetiva e
    on
        t.data_versao = e.data_versao_trips
),
quadro_trips as (
    select
        *
    from (
        select distinct
            * except(trip_id),
            trip_id as trip_id_planejado,
            trip_id
        from
            quadro
        where sentido = "I" or sentido = "V"
    )
    union all (
        select
            * except(trip_id),
            trip_id as trip_id_planejado,
            concat(trip_id, "_0") as trip_id,
        from
            quadro
        where sentido = "C"
    )
    union all (
        select
            * except(trip_id),
            trip_id as trip_id_planejado,
            concat(trip_id, "_1") as trip_id,
        from
            quadro
        where sentido = "C"
    )
),
quadro_tratada as (
    select
        q.*,
        t.shape_id as shape_id_planejado,
        case
            when sentido = "C"
            then shape_id || "_" || split(q.trip_id, "_")[offset(1)]
            else shape_id
        end as shape_id, -- TODO: adicionar no sigmob
    from
        quadro_trips q
    left join
        trips t
    on
        t.data = q.data
    and
        t.trip_id = q.trip_id_planejado
),
-- 4. Trata informações de shapes: junta trips e shapes para resgatar o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
shapes as (
    select
        e.data,
        data_versao as data_shape,
        shape_id,
        shape,
        start_pt,
        end_pt
    from
        data_efetiva e
    inner join (
        select *
        from {{ ref('subsidio_shapes_geom') }}
        {% if is_incremental() %}
        where
            data_versao in (select data_versao_shapes from data_efetiva)
        {% endif %}
    ) s
    on
        s.data_versao = e.data_versao_shapes
)
-- 5. Junta shapes e trips aos servicos planejados no quadro horário
select
    p.*,
    s.data_shape,
    s.shape,
    case
        when p.sentido = "C" and split(p.shape_id, "_")[offset(1)] = "0" then "I"
        when p.sentido = "C" and split(p.shape_id, "_")[offset(1)] = "1" then "V"
        when p.sentido = "I" or p.sentido = "V" then p.sentido
    end as sentido_shape,
    s.start_pt,
    s.end_pt,
    SAFE_CAST(NULL AS INT64) AS id_tipo_trajeto, -- Adaptação para formato da SUBSIDIO_V6
    SAFE_CAST(NULL AS STRING) AS feed_version, -- Adaptação para formato da SUBSIDIO_V6
    CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao -- Adaptação para formato da SUBSIDIO_V7
from
    quadro_tratada p
inner join
    shapes s
on
    p.shape_id = s.shape_id
and
    p.data = s.data

{% else %}

WITH
-- 1. Define datas do período planejado
  data_versao_efetiva AS (
  SELECT
    DATA,
    tipo_dia,
    subtipo_dia,
    feed_version,
    feed_start_date,
    tipo_os,
  FROM
    {{ ref("subsidio_data_versao_efetiva") }}
    -- rj-smtr-dev.projeto_subsidio_sppo.subsidio_data_versao_efetiva
  WHERE
    data BETWEEN DATE_SUB("{{ var('run_date') }}", INTERVAL 1 DAY) AND DATE("{{ var('run_date') }}"))
SELECT
  d.data,
  CASE
    WHEN subtipo_dia IS NOT NULL THEN CONCAT(tipo_dia, " - ", subtipo_dia)
    ELSE tipo_dia
  END AS tipo_dia,
  servico,
  vista,
  consorcio,
  sentido,
  distancia_planejada,
  distancia_total_planejada,
  IF(inicio_periodo IS NOT NULL AND ARRAY_LENGTH(SPLIT(inicio_periodo, ":")) = 3,
    DATETIME_ADD(
        DATETIME(
            d.data,
            PARSE_TIME("%T",
                CONCAT(
                SAFE_CAST(MOD(SAFE_CAST(SPLIT(inicio_periodo, ":")[OFFSET(0)] AS INT64), 24) AS INT64),
                ":",
                SAFE_CAST(SPLIT(inicio_periodo, ":")[OFFSET(1)] AS INT64),
                ":",
                SAFE_CAST(SPLIT(inicio_periodo, ":")[OFFSET(2)] AS INT64)
                )
            )
        ),
        INTERVAL DIV(SAFE_CAST(SPLIT(inicio_periodo, ":")[OFFSET(0)] AS INT64), 24) DAY
    ),
    NULL
  ) AS inicio_periodo,
  IF(fim_periodo IS NOT NULL AND ARRAY_LENGTH(SPLIT(fim_periodo, ":")) = 3,
    DATETIME_ADD(
        DATETIME(
            d.data,
            PARSE_TIME("%T",
                CONCAT(
                SAFE_CAST(MOD(SAFE_CAST(SPLIT(fim_periodo, ":")[OFFSET(0)] AS INT64), 24) AS INT64),
                ":",
                SAFE_CAST(SPLIT(fim_periodo, ":")[OFFSET(1)] AS INT64),
                ":",
                SAFE_CAST(SPLIT(fim_periodo, ":")[OFFSET(2)] AS INT64)
                )
            )
        ),
        INTERVAL DIV(SAFE_CAST(SPLIT(fim_periodo, ":")[OFFSET(0)] AS INT64), 24) DAY
    ),
    NULL
  ) AS fim_periodo,
  trip_id_planejado,
  trip_id,
  shape_id,
  shape_id_planejado,
  SAFE_CAST(NULL AS DATE) AS data_shape,
  shape,
  sentido_shape,
  start_pt,
  end_pt,
  id_tipo_trajeto,
  feed_version,
  CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao
FROM
  data_versao_efetiva AS d
LEFT JOIN
  {{ ref("ordem_servico_trips_shapes_gtfs") }} AS o
USING
  (feed_start_date,
   feed_version,
    tipo_dia,
    tipo_os)

{% endif %}