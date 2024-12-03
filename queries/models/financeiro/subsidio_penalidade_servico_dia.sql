{{
  config(
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    incremental_strategy="insert_overwrite",
  )
}}

WITH
  subsidio_dia AS (
  SELECT
    data,
    tipo_dia,
    consorcio,
    servico,
    MIN(pof) AS min_pof
  FROM
    {{ ref("subsidio_faixa_servico_dia") }}
    -- rj-smtr.financeiro_staging.subsidio_faixa_servico_dia
  WHERE
    data BETWEEN DATE("{{ var("start_date") }}")
    AND DATE("{{ var("end_date") }}")
  GROUP BY
    data,
    tipo_dia,
    consorcio,
    servico
  ),
  penalidade AS (
  SELECT
    data_inicio,
    data_fim,
    perc_km_inferior,
    perc_km_superior,
    IFNULL(-valor, 0) AS valor_penalidade
  FROM
    -- {{ ref("valor_tipo_penalidade") }}
    rj-smtr.dashboard_subsidio_sppo.valor_tipo_penalidade
  )
SELECT
    s.data,
    s.tipo_dia,
    s.consorcio,
    s.servico,
    SAFE_CAST(COALESCE(pe.valor_penalidade, 0) AS NUMERIC) AS valor_penalidade,
    '{{ var("version") }}' as versao,
    CURRENT_DATETIME("America/Sao_Paulo") as datetime_ultima_atualizacao
FROM
  subsidio_dia AS s
LEFT JOIN
  penalidade AS pe
ON
  s.data BETWEEN pe.data_inicio
  AND pe.data_fim
  AND s.min_pof >= pe.perc_km_inferior
  AND s.min_pof < pe.perc_km_superior