# Changelog - br_rj_riodejaneiro_bilhetagem

## [1.4.1] - 2024-08-27

### Alterado

- Adiciona parâmetro `truncate_minutes` nas constantes `BILHETAGEM_MATERIALIZACAO_PASSAGEIROS_HORA_PARAMS` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/169)

## [1.4.0] - 2024-08-26

### Alterado

- Separa materialização da `passageiros_hora` da `transacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/164)

## [1.3.1] - 2024-08-19

### Alterado

- Adiciona tabela do subsídio no exclude da materialização das tabelas do GPS do validador (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/150)

## [1.3.0] - 2024-08-05

### Adicionado

- Cria captura da tabela `linha_consorcio` do banco de dados da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

### Alterado

- Renomeia flow `SMTR: Bilhetagem Transação RioCard - Materialização` para `SMTR: Bilhetagem Controle Vinculo Validador - Materialização` e adiciona modelo `transacao_riocard` no parametro `exclude`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

## [1.2.0] - 2024-07-17

### Alterado

- Ativa schedule do flow `bilhetagem_validacao_jae` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98)
- Adequa parâmetro exclude do flow `bilhetagem_validacao_jae` para as novas queries de validação (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98)

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