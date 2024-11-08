{{
  config(
    partition_by = {
      "field": "feed_start_date",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

{% if execute %}
  {% if var("data_versao_gtfs") < var("DATA_SUBSIDIO_V11_INICIO") %}
    {% set intervalos = [
      {'inicio': '00', 'fim': '03'},
      {'inicio': '03', 'fim': '12'},
      {'inicio': '12', 'fim': '21'},
      {'inicio': '21', 'fim': '24'},
      {'inicio': '24', 'fim': '03'}
    ] %}
  {% else %}
    {% set intervalos = [
      {'inicio': '00', 'fim': '03'},
      {'inicio': '03', 'fim': '06'},
      {'inicio': '06', 'fim': '09'},
      {'inicio': '09', 'fim': '12'},
      {'inicio': '12', 'fim': '15'},
      {'inicio': '15', 'fim': '18'},
      {'inicio': '18', 'fim': '21'},
      {'inicio': '21', 'fim': '24'},
      {'inicio': '24', 'fim': '03'}
    ] %}
  {% endif %}
  {% set dias = ['dias_uteis', 'sabado', 'domingo', 'ponto_facultativo'] %}
{% endif %}


WITH
  dados AS (
  SELECT
    SAFE_CAST(data_versao AS DATE) AS data_versao,
    SAFE_CAST(tipo_os AS STRING) AS tipo_os,
    SAFE_CAST(servico AS STRING) AS servico,
    SAFE_CAST(JSON_VALUE(content, "$.consorcio") AS STRING) AS consorcio,
    {% for dia in dias %}
    {% for intervalo in intervalos %}
    {% if intervalo.inicio != '24' %}
    SAFE_CAST(JSON_VALUE(content, "$.partidas_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_{{ dia|lower }}") AS STRING) AS {{ 'partidas_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_' ~ dia|lower }},
    SAFE_CAST(JSON_VALUE(content, "$.quilometragem_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_{{ dia|lower }}") AS STRING) AS {{ 'quilometragem_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_' ~ dia|lower }},
    {% else %}
    SAFE_CAST(JSON_VALUE(content, "$.partidas_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_diaseguinte_{{ dia|lower }}") AS STRING) AS {{ 'partidas_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_diaseguinte_' ~ dia|lower }},
    SAFE_CAST(JSON_VALUE(content, "$.quilometragem_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_diaseguinte_{{ dia|lower }}") AS STRING) AS {{ 'quilometragem_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_diaseguinte_' ~ dia|lower }},
    {% endif %}
    {% endfor %}
    {% endfor %}
  FROM
    {{ source("br_rj_riodejaneiro_gtfs_staging", "ordem_servico_faixa_horaria") }}
  {% if is_incremental() -%}
  WHERE
    data_versao = '{{ var("data_versao_gtfs") }}'
  {%- endif %}
  ),
  dados_agrupados AS (
  SELECT
    data_versao,
    tipo_os,
    servico,
    consorcio,
    CASE
      WHEN column_name LIKE '%dias_uteis%' THEN 'Dia Útil'
      WHEN column_name LIKE '%sabado%' THEN 'Sabado'
      WHEN column_name LIKE '%domingo%' THEN 'Domingo'
      WHEN column_name LIKE '%ponto_facultativo%' THEN 'Ponto Facultativo'
    END AS tipo_dia,
    CASE
      {% for intervalo in intervalos %}
      {% if intervalo.inicio != '24' %}
      WHEN column_name LIKE '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h%' THEN '{{ intervalo.inicio }}:00:00'
      {% else %}
      WHEN column_name LIKE '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_diaseguinte%' THEN '{{ intervalo.inicio }}:00:00'
      {% endif %}
      {% endfor %}
    END AS faixa_horaria_inicio,
    CASE
      {% for intervalo in intervalos %}
      {% if intervalo.inicio != '24' %}
      WHEN column_name LIKE '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h%' THEN '{{ '%02d'|format(intervalo.fim|int - 1) }}:59:59'
      {% else %}
      WHEN column_name LIKE '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_diaseguinte%' THEN '26:59:59'
      {% endif %}
      {% endfor %}
    END AS faixa_horaria_fim,
    SUM(CASE
        WHEN column_name LIKE '%partidas%' THEN SAFE_CAST(value AS INT64)
        ELSE 0
    END) AS partidas,
    SUM(CASE
        WHEN column_name LIKE '%quilometragem%' THEN SAFE_CAST(value AS FLOAT64)
        ELSE 0
    END) AS quilometragem
  FROM dados
  UNPIVOT (
    value FOR column_name IN (
      {% for dia in dias %}
      {% for intervalo in intervalos %}
      {% if intervalo.inicio != '24' %}
      {{ 'partidas_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_' ~ dia|lower }},
      {{ 'quilometragem_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_' ~ dia|lower }},
      {% else %}
      {{ 'partidas_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_diaseguinte_' ~ dia|lower }},
      {{ 'quilometragem_entre_' ~ intervalo.inicio ~ 'h_e_' ~ intervalo.fim ~ 'h_diaseguinte_' ~ dia|lower }}
      {% endif %}
      {% endfor %}
      {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  )
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
  fi.feed_version,
  fi.feed_start_date,
  fi.feed_end_date,
  d.* EXCEPT(data_versao),
  '{{ var("version") }}' AS versao_modelo
FROM
  dados_agrupados AS d
LEFT JOIN
  {{ ref('feed_info_gtfs') }} AS fi
ON
  d.data_versao = fi.feed_start_date
{% if is_incremental() -%}
WHERE
  d.data_versao = '{{ var("data_versao_gtfs") }}'
  AND fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
{% else %}
WHERE
  d.data_versao >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
{%- endif %}
