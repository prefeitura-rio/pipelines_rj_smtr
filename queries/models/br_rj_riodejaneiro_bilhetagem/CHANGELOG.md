# Changelog - bilhetagem

## [2.2.0] - 2024-08-05

### Adicionado
- Cria modelo `aux_h3_res9.sql` para tratar os dados geográficos de tile (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)
- Cria modelo `staging_linha_consorcio.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

### Alterado
- `aux_passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)
  - Faz conversão dos dados geograficos no modelo
  - Altera materialized de `ephemeral` para `table`
  - Adiciona transações do tipo RioCard
- Muda tipo das colunas de valor do modelo `staging_transacao_riocard.sql` de `float` para `numeric` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)
- Altera lógica incremental do modelo `transacao_riocard.sql` para rodar de hora em hora (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

## [2.1.4] - 2024-08-02

### Alterado
- Adiciona tag `geolocalizacao` aos modelos `gps_validador_van.sql` e `gps_validador.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)
- Adiciona tag `identificacao` ao modelo `staging_cliente.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)

## [2.1.3] - 2024-07-18

### Adicionado
- Adiciona transações de Van no modelo `passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/110)

### Alterado
- Define o tipo_gratuidade de transações do tipo `Gratuidade` que o cliente não foi encontrado na tabela `br_rj_riodejaneiro_bilhetagem_staging.aux_gratuidade` como `Não Identificado` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/110)

## [2.1.2] - 2024-07-05

### Adicionado
- Adiciona coluna `versao_app` nos modelos `gps_validador_aux.sql`, `gps_validador.sql` e `gps_validador_van.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/96)

## [2.1.1] - 2024-06-19

### Corrigido
- Remove filtro de partições de gratuidade no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/69)

## [2.1.0] - 2024-06-13

### Alterado
- Adiciona colunas `data_pagamento` e `valor_pago` no modelo `ordem_pagamento_consorcio_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)
- Subtrai valor pago a mais ao VLT (data_ordem = 2024-05-31) da ordem do dia 2024-06-07 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)
- Adiciona prioridade dos dados novos em relação aos antigos no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)

## [2.0.0] - 2024-06-11

### Adicionado
- Cria modelos `passageiros_hora_aux.sql` e `passageiros_tile_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)

### Alterado
- Adiciona colunas tipo_transacao_smtr e valor_pagamento no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)
- Altera lógica da execução incremental no modelo `transacao.sql` para atualizar valores de rateio e o tipo de transação da primeira perna das integrações (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)

### Corrigido
- Corrige filtro no modelo `passageiros_hora_aux.sql` para adicionar transações de Ônibus (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)

## [1.2.1] - 2024-05-20

### Corrigido
- Altera alias da tabela `linha_sem_ressarcimento` no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/317)
- Corrige select servico no modelo `ordem_pagamento_servico_operador_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/317)

## [1.2.0] - 2024-05-20

### Alterado
- Adiciona colunas `servico_jae` e `descricao_servico_jae` nos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311):
  - `transacao.sql`
  - `integracao.sql`
  - `ordem_pagamento_servico_operador_dia.sql`
  - `passageiros_hora.sql`
  -  `gps_validador.sql`
  -  `gps_validador_van.sql`
  -  `staging/gps_validador_aux.sql`
- Adiciona coluna id_servico_jae nos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311):
  -  `gps_validador.sql`
  -  `gps_validador_van.sql`
  -  `staging/gps_validador_aux.sql`

- Remove coluna `servico` no modelo de `staging/gps_validador_aux.sql` para pegar o dado da tabela de cadastro

## [1.1.0] - 2024-05-16

### Alterado
- Adiciona tratamento da coluna id_veiculo nos modelos ` transacao.sql` e `gps_validador.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297)
- Adiciona coluna `quantidade_total_transacao` nos modelos `ordem_pagamento_consorcio_dia.sql`, `ordem_pagamento_consorcio_operador_dia.sql` e `ordem_pagamento_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297)
- Remove validação do modelo `ordem_pagamento_servico_operador_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297)

## [1.0.3] - 2024-05-03

### Corrigido
- Removido tratamento de arredondamento nos valores totais (https://github.com/prefeitura-rio/queries-rj-smtr/pull/294):
  - `bilhetagem.ordem_pagamento_dia`
  - `bilhetagem.ordem_pagamento_consorcio_operador_dia`
  - `bilhetagem.ordem_pagamento_consorcio_dia`

### Alterado
- Alterado cast de float para numeric (https://github.com/prefeitura-rio/queries-rj-smtr/pull/294):
  - `bilhetagem_staging.staging_ordem_pagamento`
  - `bilhetagem_staging.staging_ordem_pagamento_consorcio`
  - `bilhetagem_staging.staging_ordem_pagamento_consorcio_operadora`
  - `bilhetagem_staging.staging_ordem_rateio`
  - `bilhetagem_staging.staging_ordem_ressarcimento`

## [1.0.2] - 2024-04-18

### Alterado
- Filtra transações inválidas ou de teste no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/275)
  - Transações inválidas:
    - datas anteriores a 2023-07-17
  - Transações teste:
    - linhas sem ressarcimento
- Limita quantidade de ids listados no filtro da tabela de gratuidades no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/275)

## [1.0.1] - 2024-04-16

### Adicionado
- Adicionada descrições das colunas das tabelas: `ordem_pagamento_consorcio_dia`, `ordem_pagamento_consorcio_operador_dia`, `ordem_pagamento_dia`,`transacao_riocard`
- Adicionada a descrição da coluna intervalo_integracao na tabela `integracao`

### Corrigido
- deletada a tabela ordem_pagamento do schema

## [1.0.0] - 2024-04-05

### Adicionado
- Nova view para consultar os dados staging de transações do RioCard capturados pela Jaé: `br_rj_riodejaneiro_bilhetagem_staging/staging_transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)
- Tabela tratada de transações do RioCard capturados pela Jaé: `transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)
