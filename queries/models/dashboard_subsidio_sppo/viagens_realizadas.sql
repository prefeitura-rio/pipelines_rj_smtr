SELECT
  *
FROM
  rj-smtr.projeto_subsidio_sppo.viagem_completa
WHERE
  data BETWEEN "2022-06-01" AND DATE("{{ var("end_date") }}")