# Changelog - br_rj_riodejaneiro_bilhetagem

## [1.4.8] - 2024-12-16

### Alterado
- Altera IP do banco de tracking da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/362)

## [1.4.7] - 2024-09-16

### Alterado
- Altera formato de data do campo data_pagamento do modelo `staging_ordem_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/233)

## [1.4.6] - 2024-09-16

### Alterado
- Muda IP do banco ressarcimento_db de `10.5.15.127` para `10.5.12.50` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/228)

## [1.4.5] - 2024-09-13

### Alterado
- Cria tolerância para pular a run no State Handler do flow `bilhetagem_tracking_captura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/222)

## [1.4.4] - 2024-09-12
s
### Corrigido

`bilhetagem_tracking_captura`:
    - Remove try/catch da criação de parâmetros (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/216)
    - Faz o cast do id para inteiro para definir o id inicial do filtro (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/216)

## [1.4.3] - 2024-08-29

### Corrigido

- Remove verificação de sources da materialização da`passageiros_hora` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/173)

## [1.4.2] - 2024-08-28

### Corrigido

- Remove parâmetro timestamp da materialização da `passageiros_hora` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/172)

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