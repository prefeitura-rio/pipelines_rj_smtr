# Changelog - validacao_dados_jae

## [1.1.1] - 2024-08-21

### Corrigido
  - Remove pernas nulas no modelo `integracao_nao_realizada.sql`

## [1.1.0] - 2024-08-21

### Adicionado
  - Cria modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/157)

## [1.0.0] - 2024-07-17

### Adicionado
  - Cria modelos para validação dos dados da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98):
    - `aux_transacao_ordem.sql`
    - `ordem_pagamento_servico_operador_dia_invalida.sql`
    - `ordem_pagamento_consorcio_operador_dia_invalida.sql`
    - `ordem_pagamento_consorcio_dia_invalida.sql`
    - `ordem_pagamento_dia_invalida.sql`
    - `integracao_invalida.sql`
    - `transacao_invalida.sql`
    - `ordem_pagamento_invalida.sql`