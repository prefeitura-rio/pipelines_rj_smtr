-- depends_on: {{ ref('view_passageiros_hora') }}
WITH servicos AS (
  SELECT
    * EXCEPT(rn)
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_servico_jae ORDER BY data_inicio_vigencia) AS rn
      FROM
        {{ ref("servicos") }}
    )
  WHERE
    rn = 1
)
SELECT
  p.data,
  p.hora,
  p.modo,
  p.consorcio,
  p.id_servico_jae,
  s.servico,
  s.descricao_servico,
  CONCAT(s.servico, ' - ' ,s.descricao_servico) AS nome_completo_servico,
  s.latitude AS latitude_servico,
  s.longitude AS longitude_servico,
  p.sentido,
  p.quantidade_passageiros
FROM
  {{ ref("passageiros_hora") }} p
LEFT JOIN
  servicos s
USING(id_servico_jae)
WHERE
  tipo_transacao_smtr = "RioCard"
  AND modo IS NOT NULL