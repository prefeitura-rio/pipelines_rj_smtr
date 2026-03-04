SELECT
  *
FROM
  `rj-smtr-dev.janaina__SMTR202511005101__projeto_subsidio_sppo.viagem_completa`
WHERE
  data BETWEEN "2022-06-01" AND DATE("{{ var("end_date") }}")