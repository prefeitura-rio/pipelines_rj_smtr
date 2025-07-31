# Changelog - queries

## [1.0.7] - 2025-07-31

### Alterado

- Altera selector `apuracao_subsidio_v9` excluindo o modelo `aux_viagem_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Altera selector `monitoramento_veiculo` adicionando os modelos `aux_viagem_temperatura`, `temperatura_inmet`, `aux_veiculo_falha_ar_condicionado`, `veiculo_regularidade_temperatura_dia` e `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

## [1.0.6] - 2025-07-09

### Alterado

- Altera selector `apuracao_subsidio_v9` para executar todos os modelos do dataset `subsidio` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

## [1.0.5] - 2025-06-27

### Alterado

- Altera selector `apuracao_subsidio_v9` adicionando o modelo `percentual_operacao_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)

## [1.0.4] - 2025-05-20

### Adicionado

- Adiciona selector `gps` e `gps_15_minutos` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/297)

## [1.0.3] - 2025-05-13

### Alterado

- Altera selector `planejamento_diario` adicionando o modelo `servico_planejado_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/575)

## [1.0.2] - 2025-04-08

### Alterado

- Altera selector `apuracao_subsidio_v9` adicionando o modelo `staging_tecnologia_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/530)

## [1.0.1] - 2025-01-28

### Alterado

- Altera selector `apuracao_subsidio_v9` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

## [1.0.0] - 2024-10-29

### Adicionado

- Adiciona package: `dbt-labs/dbt_utils` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/288)