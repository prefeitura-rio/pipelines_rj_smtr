{{
  config(
    partition_by = {
    "field": "data",
    "data_type": "date",
    "granularity": "month"
    },
)}}

WITH consorcio AS (
  SELECT
    id_consorcio,
    modo
  FROM
    {{ ref("consorcios") }}
    -- rj-smtr.cadastro.consorcios
  WHERE
    modo IN ("Ônibus", "BRT")
)
SELECT
  DATE_TRUNC(data, MONTH) AS data,
  rdo.ano,
  rdo.mes,
  c.modo,
  SUM(rdo.qtd_grt_idoso + rdo.qtd_grt_especial +
      rdo.qtd_grt_estud_federal + rdo.qtd_grt_estud_estadual +
      rdo.qtd_grt_estud_municipal + rdo.qtd_grt_rodoviario +
      rdo.qtd_grt_passe_livre_universitario) AS quantidade_passageiro_gratuidade_mes,
  CURRENT_DATE("America/Sao_Paulo") AS data_ultima_atualizacao,
  '{{ var("version") }}' as versao
FROM
  consorcio AS c
LEFT JOIN
  {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }} AS rdo
ON
  rdo.termo = c.id_consorcio
WHERE
  rdo.data >= "2015-01-01"
  {% if is_incremental() %}
  AND rdo.data BETWEEN DATE_TRUNC(DATE("{{ var("start_date") }}"), MONTH)
  AND LAST_DAY(DATE("{{ var("end_date") }}"), MONTH)
  AND rdo.data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)
  {% endif %}
GROUP BY
  data,
  rdo.ano,
  rdo.mes,
  c.modo
