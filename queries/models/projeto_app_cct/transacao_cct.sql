SELECT
  id_transacao,
  data,
  datetime_transacao,
  consorcio,
  valor_pagamento,
  id_ordem_pagamento,
  id_ordem_pagamento_consorcio_operador_dia,
  datetime_ultima_atualizacao
FROM
  {{ ref("transacao") }}
WHERE
  id_ordem_pagamento_consorcio_operador_dia IS NOT null
