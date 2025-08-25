select
    id_transacao,
    data,
    datetime_transacao,
    consorcio,
    tipo_transacao,
    tipo_transacao_jae,
    valor_pagamento,
    id_ordem_pagamento,
    id_ordem_pagamento_consorcio_operador_dia,
    datetime_ultima_atualizacao
from {{ ref("transacao") }}
where id_ordem_pagamento_consorcio_operador_dia is not null and modo = 'Van'
