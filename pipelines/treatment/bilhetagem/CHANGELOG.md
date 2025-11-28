# Changelog - bilhetagem

## [2.0.14] - 2025-11-19

### Adicionado

- Adiciona `aux_gratuidade_info` nos testes diários da transação e `sincronizacao_tabelas__transacao_gratuidade_estudante_municipal` no exclude (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)
- Adiciona parâmetro `test_webhook_key` nos flows de materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

### Alterado

- Altera schedule da materialização da `passageiro_hora` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

## [2.0.13] - 2025-11-05

### Adicionado

- Adiciona tolerância no handler skip if running da materialização da`transacao` e do `extrato_cliente_cartao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)

## [2.0.12] - 2025-10-02

### Adicionado

- Adiciona novo selector `EXTRATO_CLIENTE_CARTAO_SELECTOR` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)
- Adiciona novo flow `EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)

### Removido

- Remove modelo `recarga_jae.sql` do selector de transacao e extrato (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)

## [2.0.11] - 2025-09-29

### Adicionado

- Adiciona execuções de fallback nos flows `INTEGRACAO_MATERIALIZACAO` e `TRANSACAO_ORDEM_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

### Alterado

- Altera materialização da integracao de 8:00 para 10:30 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Altera materialização da transacao_ordem de 9:00 para 11:00 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Altera materialização da TRANSACAO_VALOR_ORDEM_SELECTOR de 11:30 para 12:00 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Altera teste da transacao de 11:15 para 12:00 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [2.0.10] - 2025-09-03

### Adicionado

- Adiciona teste `dbt_utils.expression_is_true__transacao_valor_ordem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/836)

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