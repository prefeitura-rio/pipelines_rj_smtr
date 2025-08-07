/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/

{{
  config(
    materialized='ephemeral'
  )
}}

-- 1. Busca anexo de trajetos alternativos
WITH
  ordem_servico_trajeto_alternativo AS (
    SELECT
      *
    FROM
      {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
    {% if is_incremental() -%}
      WHERE
        feed_start_date = "{{ var('data_versao_gtfs') }}"
    {%- endif %}
  ),
  ordem_servico_faixa_horaria_sentido as (
        select 
        feed_start_date,
        servico,
        ARRAY_AGG(DISTINCT left(sentido, 1)) AS sentido_array,
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        {% if is_incremental() -%}
            where feed_start_date = '{{ var("data_versao_gtfs") }}'
        {% endif -%}
        GROUP BY
        1,
        2
    ),
  -- 2. Despivota anexo de trajetos alternativos
  ordem_servico_trajeto_alternativo_sentido AS (
    SELECT
      *
    FROM
      ordem_servico_trajeto_alternativo
    UNPIVOT
    (
      (
        distancia_planejada
      ) FOR sentido IN (
        (
          extensao_ida
        ) AS "I",
        (
          extensao_volta
        ) AS "V"
      )
    )
  )
-- 3. Atualiza sentido dos serviços circulares no anexo de trajetos alternativos
SELECT
    * EXCEPT(sentido),
    CASE
        WHEN "C" IN UNNEST(sentido_array) THEN "C"
        ELSE o.sentido
    END AS sentido,
FROM
    ordem_servico_trajeto_alternativo_sentido AS o
LEFT JOIN
    ordem_servico_faixa_horaria_sentido AS s
USING
    (feed_start_date, servico)
WHERE
    distancia_planejada != 0