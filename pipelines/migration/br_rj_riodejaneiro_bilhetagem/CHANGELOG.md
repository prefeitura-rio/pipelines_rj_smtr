# Changelog - br_rj_riodejaneiro_bilhetagem

## [1.1.0] - 2024-06-13

### Adicionado

- Adiciona data check da ordem de pagamento no final da materialização no flow `bilhetagem_ordem_pagamento_captura_tratamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)

### Alterado

- Adiciona

## [1.0.2] - 2024-05-28

### Alterado

- Remove schedule do flow `bilhetagem_validacao_jae`, por conta da arquitetura das tabelas que será alterado (https://github.com/prefeitura-rio/pipelines/pull/691)

## [1.0.1] - 2024-05-22

### Corrigido

- Corrige exclude nos parâmetros da pipeline `bilhetagem_validacao_jae` (https://github.com/prefeitura-rio/pipelines/pull/689)

## [1.0.0] - 2024-05-17

### Alterado

- Adiciona +servicos no exclude geral dos pipelines de materialização da bilhetagem (https://github.com/prefeitura-rio/pipelines/pull/685)

### Corrigido

- Corrige parametros do flow `bilhetagem_validacao_jae` (https://github.com/prefeitura-rio/pipelines/pull/685)
- Adiciona tabelas de validação no exclude do flow `bilhetagem_materializacao_gps_validador` (https://github.com/prefeitura-rio/pipelines/pull/685)