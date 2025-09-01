# Changelog - bilhetagem

## [2.0.9] - 2025-09-01

### Removido

- Move materialização do `gps_validador` para o dataset `monitoramento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [2.0.8] - 2025-08-22

### Adicionado
- Adiciona `ordem_ressarcimento` no wait do flow `INTEGRACAO_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/807)

### Alterado
- Altera schedule da materialização da integracao (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/807)

## [2.0.7] - 2025-08-19

### Adicionado

- Adiciona `post_tests` nos flows (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783):
  - `TRANSACAO_MATERIALIZACAO`
  - `INTEGRACAO_MATERIALIZACAO`
  - `GPS_VALIDADOR_MATERIALIZACAO`
  - `PASSAGEIRO_HORA_MATERIALIZACAO`
  - `TRANSACAO_VALOR_ORDEM_MATERIALIZACAO`

- Adiciona `test_scheduled_time` nos flows (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783):
  - `TRANSACAO_MATERIALIZACAO`
  - `PASSAGEIRO_HORA_MATERIALIZACAO`
  - `GPS_VALIDADOR_MATERIALIZACAO`

## [2.0.6] - 2025-08-11

### Alterado

- Altera schedule da materialização da transacao_ordem e integracao (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/781)

## [2.0.5] - 2025-07-31

### Alterado

- Altera schedule da materialização da transacao_ordem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/743)

## [2.0.4] - 2025-07-30

### Alterado

- Altera schedule da materialização da transacao_ordem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/740)

## [2.0.3] - 2025-07-29

### Adicionado

- Adiciona source da tabela `lancamento` da Jaé no wait do flow `TRANSACAO_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/735)

## [2.0.2] - 2025-07-03

### Alterado

- Agenda execução do flow `TRANSACAO_VALOR_ORDEM_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/658)

## [2.0.1] - 2025-05-05

### Adicionado

- Adiciona `handler_notify_failure` nos flows (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/559):
  - `TRANSACAO_MATERIALIZACAO`
  - `INTEGRACAO_MATERIALIZACAO`
  - `GPS_VALIDADOR_MATERIALIZACAO`
  - `TRANSACAO_ORDEM_MATERIALIZACAO`

## [2.0.0] - 2025-03-26

### Adicionado

- Migra flows de tratamento da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/489)

## [1.1.0] - 2025-02-04

### Adicionado

- Cria flow de tratamento da tabela `transacao_valor_ordem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/411)

## [1.0.0] - 2024-11-25

### Adicionado

- Cria flow de tratamento da tabela auxiliar `aux_transacao_id_ordem_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/333)