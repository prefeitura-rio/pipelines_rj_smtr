# Changelog - financeiro

## [2.1.1] - 2025-11-19

### Adicionado

- Adiciona parâmetro `test_webhook_key` no flow de materialização de ordens de pagamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

## [2.1.0] - 2025-10-09

### Adicionado

- Cria flow `PAGAMENTO_CCT_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/934)

## [2.0.7] - 2025-10-02

### Alterado

- Substitui função de leitura do BigQuery da BD pela do pandas_gbq (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/918)

## [2.0.6] - 2025-09-29

### Alterado

- Altera schedule do flow `ordem_pagamento_quality_check` de 9:00 para 10:15 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Altera reference_tasks do flow `ordem_pagamento_quality_check` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Altera materialização das ordens de pagamento de 8:30 para 10:15 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [2.0.5] - 2025-09-01

### Alterado

- Altera referências dos modelos de ordem de pagamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [2.0.4] - 2025-08-11

### Alterado

- Altera schedule da materialização das ordens de pagamento da bilhetagem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/781)
- Altera webhook do flow `FINANCEIRO_BILHETAGEM_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/781)

## [2.0.3] - 2025-07-31

### Alterado

- Altera schedule da materialização das ordens de pagamento da bilhetagem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/743)

## [2.0.2] - 2025-07-30

### Alterado

- Altera schedule da materialização das ordens de pagamento da bilhetagem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/740)

## [2.0.1] - 2025-05-05

## Adicionado

- Adiciona `handler_notify_failure` no flow `FINANCEIRO_BILHETAGEM_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/559)

## [2.0.0] - 2025-03-26

## Adicionado

- Migra flows de tratamento da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/489)

## [1.0.0] - 2025-03-13

### Adicionado

- Cria flow de teste de qualidade dos dados de ordem de pagamento `ordem_pagamento_quality_check` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/474)
