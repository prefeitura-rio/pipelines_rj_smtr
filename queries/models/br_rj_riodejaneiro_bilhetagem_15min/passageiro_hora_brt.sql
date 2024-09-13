{{
  config(
    materialized="table"
  )
}}
-- depends_on: {{ ref('gps_validador_15min') }}
{% set descricao_servico %}
  CASE
    WHEN
      descricao_servico_jae = "Terminal Jardim Oceânico - Mezainino"
      AND id_validador IN (
        "B35T020D00200518",
        "B35T020D00200513",
        "B35T020D00200504",
        "B35T020D00200406",
        "B35T020D00200405",
        "B35T020D00200403",
        "B35T020D00200376",
        "B35T020D00200203",
        "B35T020D00200199",
        "B35T020D00200152",
        "B35T020D00200108",
        "B35T020D00200107",
        "B35T020D00200104",
        "B35T020D00200103",
        "B35T020D00200068",
        "B35T020D00200011",
        "B35T020D00200010"
      )
    THEN "Terminal Jardim Oceânico (Metrô)"
    WHEN descricao_servico_jae = "Terminal Jardim Oceânico - Mezainino" THEN "Terminal Jardim Oceânico (exceto Metrô)"
    ELSE descricao_servico_jae
  END
{% endset %}

WITH transacao AS (
  SELECT
    data,
    hora,
    {{ descricao_servico }} AS descricao_servico,
    COUNT(*) AS quantidade_passageiros_jae
  FROM
    {{ ref("transacao_15min") }}
  WHERE
    data >= "2024-09-13"
    AND modo = "BRT"
  GROUP BY
    1,
    2,
    3
),
transacao_riocard AS (
    SELECT
      data,
      hora,
      {{ descricao_servico }} AS descricao_servico,
      COUNT(*) AS quantidade_passageiros_riocard
    FROM
      {{ ref("transacao_riocard_15min") }}
    WHERE
      data >= "2024-09-13"
      AND modo = "BRT"
    GROUP BY
      1,
      2,
      3
)
SELECT
  data,
  hora,
  descricao_servico,
  IFNULL(t.quantidade_passageiros_jae, 0) AS quantidade_passageiros_jae,
  IFNULL(tr.quantidade_passageiros_riocard, 0) AS quantidade_passageiros_riocard,
  IFNULL(t.quantidade_passageiros_jae, 0) + IFNULL(tr.quantidade_passageiros_riocard, 0) AS quantidade_passageiros_total
FROM
  transacao t
FULL OUTER JOIN
  transacao_riocard tr
USING(
  data,
  hora,
  descricao_servico
)