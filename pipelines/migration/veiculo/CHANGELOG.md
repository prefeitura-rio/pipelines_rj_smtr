# Changelog - veiculo

## [1.1.5] - 2025-03-21

### Alterado
- Alterado o flow de materialização de veículo para períodos inteiros de uma vez (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/434)

## [1.1.4] - 2025-02-10

### Adicionado
- Adicionados novos campos na constante `SPPO_LICENCIAMENTO_MAPPING_KEYS` conforme novo leiaute informado pela IPLANRIO/PRE/DPN/GTIS-6 por e-mail em 2025-02-10 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/420)

## [1.1.3] - 2025-02-06

### Adicionado
- Adicionados testes no modelo `infracao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

## [1.1.2] - 2025-02-05

### Alterado
- Alterados os schedules dos flows de captura de `infracao` e `licenciamento_stu` para `todos os dias às 06:50` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/415)

## [1.1.1] - 2025-01-23

### Alterado
- Remove parâmetro `stu_data_versao` do flow `sppo_veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/395)

## [1.1.0] - 2025-01-16

### Alterado
- Alterações no tratamento do arquivo de infrações (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/126):
  - Remove coluna `placa` das primary keys
  - Remove filtro de modo

## [1.0.1] - 2024-05-28

### Adicionado

- Adiciona retry a task `get_raw_ftp` para mitigar as falhas na captura (https://github.com/prefeitura-rio/pipelines/pull/694)

## [1.0.0] - 2024-04-25

### Alterado

- Desliga schedule dos flows `sppo_infracao_captura` e `sppo_licenciamento_captura` em razão de indisponibilidade e geração de dados imprecisos na fonte (SIURB) (https://github.com/prefeitura-rio/pipelines/pull/672)