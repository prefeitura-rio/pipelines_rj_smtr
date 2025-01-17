# Changelog - financeiro

## [1.0.2] - 2025-01-21

### Adicionado

- Cria modelo `subsidio_penalidade_servico_faixa` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

### Removido

- Remove modelo `subsidio_penalidade_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [1.0.1] - 2024-09-20

### Alterado

- Altera `km_subsidiada_faixa` para considerar kms com indicador_viagem_dentro_limite = TRUE, pof >= 80 e subsidio_km > 0 no modelo `subsidio_faixa_servico_dia_tipo_viagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/239)

## [1.0.0] - 2024-08-29

### Adicionado

- Cria modelos `subsidio_penalidade_servico_dia`, `subsidio_faixa_servico_dia_tipo_viagem` e `subsidio_sumario_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)