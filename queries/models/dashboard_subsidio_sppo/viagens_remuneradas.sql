{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}

{%- if execute %}
  {% set query = "SELECT DISTINCT COALESCE(feed_start_date, data_versao_trips, data_versao_shapes, data_versao_frequencies) FROM " ~ ref('subsidio_data_versao_efetiva') ~ " WHERE data BETWEEN DATE('" ~ var('start_date') ~ "') AND DATE('" ~ var("end_date") ~ "')"%}
  {{- log(query, info=True) -}}
  {% set feed_start_dates = run_query(query).columns[0].values() %}
  {{- log(feed_start_dates, info=True) -}}
{% endif -%}

WITH
-- 1. Viagens planejadas (agrupadas por data e serviço)
  planejado AS (
  SELECT
    DISTINCT data,
    tipo_dia,
    consorcio,
    servico,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    distancia_total_planejada AS km_planejada, -- ADD SUM(distancia_total_planejada) e GROUP BY 1,2,3,4
  FROM
    {{ ref("viagem_planejada") }}
    -- rj-smtr.projeto_subsidio_sppo.viagem_planejada
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
    AND ( distancia_total_planejada > 0
      OR distancia_total_planejada IS NULL )
    AND (id_tipo_trajeto = 0
      OR id_tipo_trajeto IS NULL)
  ),
  viagens_planejadas AS (
  SELECT
    feed_start_date,
    servico,
    tipo_dia,
    viagens_planejadas,
    partidas_ida,
    partidas_volta,
    tipo_os,
  FROM
      {{ ref("ordem_servico_gtfs") }}
      -- rj-smtr.gtfs.ordem_servico
  WHERE
    feed_start_date IN ('{{ feed_start_dates|join("', '") }}')
  ),
  data_versao_efetiva AS (
  SELECT
    data,
    tipo_dia,
    tipo_os,
    COALESCE(feed_start_date, data_versao_trips, data_versao_shapes, data_versao_frequencies) AS feed_start_date
  FROM
      {{ ref("subsidio_data_versao_efetiva") }}
      -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva -- (alterar também query no bloco execute)
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
  ),
  viagem_planejada AS (
  SELECT
    p.*,
    viagens_planejadas,
    v.partidas_ida + v.partidas_volta AS viagens_planejadas_ida_volta
  FROM
    planejado AS p
  LEFT JOIN
    data_versao_efetiva AS d
  USING
    (data, tipo_dia)
  LEFT JOIN
    viagens_planejadas AS v
  ON
    d.feed_start_date = v.feed_start_date
    AND p.tipo_dia = v.tipo_dia
    AND p.servico = v.servico
    AND (d.tipo_os = v.tipo_os
      OR (d.tipo_os IS NULL AND v.tipo_os = "Regular"))
  ),
-- 2. Parâmetros de subsídio
  subsidio_parametros AS (
  SELECT
    DISTINCT data_inicio,
    data_fim,
    status,
    subsidio_km,
    MAX(subsidio_km) OVER (PARTITION BY data_inicio, data_fim) AS subsidio_km_teto
  FROM
    {{ ref("subsidio_valor_km_tipo_viagem") }}
    -- rj-smtr-staging.dashboard_subsidio_sppo_staging.subsidio_valor_km_tipo_viagem
),
-- 3. Viagens com quantidades de transações
  viagem_transacao AS (
  SELECT
    *
 FROM
    {{ ref("viagem_transacao") }}
    -- rj-smtr.subsidio.viagem_transacao
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE( "{{ var("end_date") }}" )
  ),
-- 4. Viagens com tipo e valor de subsídio por km
  viagem_km_tipo AS (
  SELECT
    vt.data,
    vt.servico,
    vt.tipo_viagem,
    vt.id_viagem,
    vt.datetime_partida,
    vt.distancia_planejada,
    t.subsidio_km,
    t.subsidio_km_teto
  FROM
    viagem_transacao AS vt
  LEFT JOIN
    subsidio_parametros AS t
  ON
    vt.data BETWEEN t.data_inicio
    AND t.data_fim
    AND vt.tipo_viagem = t.status ),
-- 5. Apuração de km realizado e Percentual de Operação Diário (POD)
  servico_faixa_km_apuracao AS (
    SELECT
      *
    FROM
      {{ ref("subsidio_faixa_servico_dia") }}
)
-- 6. Flag de viagens que serão consideradas ou não para fins de remuneração (apuração de valor de subsídio) - RESOLUÇÃO SMTR Nº 3645/2023
SELECT
  v.* EXCEPT(rn),
  CASE
    WHEN v.tipo_viagem = "Sem transação"
      THEN FALSE
    WHEN data >= "2023-09-16"
        AND p.tipo_dia = "Dia Útil"
        AND viagens_planejadas > 10
        AND pof > 120
        AND rn > viagens_planejadas_ida_volta*1.2
        THEN FALSE
    WHEN data >= "2023-09-16"
        AND p.tipo_dia = "Dia Útil"
        AND viagens_planejadas <= 10
        AND pof > 200
        AND rn > viagens_planejadas_ida_volta*2
        THEN FALSE
    WHEN data >= "2023-09-16"
        AND (p.tipo_dia = "Dia Útil"
          AND (viagens_planejadas IS NULL
            OR pof IS NULL
            OR rn IS NULL
            OR viagens_planejadas_ida_volta IS NULL
          )
        )
      )
      THEN NULL
    ELSE
        TRUE
    END AS indicador_viagem_remunerada
FROM (
SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY data, servico ORDER BY subsidio_km*distancia_planejada DESC) AS rn
FROM
    viagem_km_tipo ) AS v
LEFT JOIN
    viagem_planejada AS p
ON
  p.data = v.data
  AND p.servico = v.servico
  AND v.datetime_partida BETWEEN p.faixa_horaria_inicio
  AND p.faixa_horaria_fim
LEFT JOIN
    servico_faixa_km_apuracao AS s
ON
  s.data = v.data
  AND s.servico = v.servico
  AND v.datetime_partida BETWEEN s.faixa_horaria_inicio
  AND s.faixa_horaria_fim