# Changelog - validacao_dados_jae

## [3.0.0] - 2025-08-19


# Adicionado

- Cria nova lógica para o cálculo de integrações não realizadas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/793):
  - `aux_particao_calculo_integracao.sql`
  - `aux_transacao_filtro_integracao_calculada.sql`
  - `aux_calculo_integracao.py`
  - `aux_integracao_calculada.sql`
  - `integracao_nao_realizada.sql`

## [2.0.0] - 2024-12-30

### Alterado

- Altera modelos `integracao_nao_realizada.sql` e `integracao_invalida.sql` para considerar a matriz publicada pela SMTR (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)
- Altera variável de data dos modelos `integracao_nao_realizada.sql` e `integracao_invalida.sql` de `run_date` para `date_range_[start/end]` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)

## [1.1.3] - 2024-09-04

### Alterado
  - Modelo `integracao_nao_realizada.sql`:
    - Soma 1 na coluna `sequencia_integracao` para padronizar em relação a tabela `integracao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/185)
    - Cria filtro para remover integrações com 2 ou mais transações do modo `BRT` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/185)

## [1.1.2] - 2024-08-27

### Corrigido
  - Remove transações do tipo gratuidade no modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/169)

## [1.1.1] - 2024-08-21

### Corrigido
  - Remove pernas nulas no modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/159)
  -
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