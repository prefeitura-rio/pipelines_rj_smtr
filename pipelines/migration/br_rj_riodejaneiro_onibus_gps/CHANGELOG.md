# Changelog - br_rj_riodejaneiro_onibus_gps

## [1.0.4] - 2025-02-06

### Corrigido

- Corrige as descrições de testes do modelo `gps_sppo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

## [1.0.3] - 2024-10-29

### Alterado

- Altera o flow `materialize_sppo` para utilizar as tasks que rodam os testes do DBT (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/288)

## [1.0.2] - 2024-08-25

### Adicionado

- Cria arquivo `constants.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/287)

### Alterado

- Altera a task `get_raw` para verificar se a captura está vazia (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/287)

## [1.0.1] - 2024-08-19

### Alterado
- Alterado os default parameters do flow materialize_gps_15_min (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/149)

## [1.0.0] - 2024-08-07

### Adicionado
- Migra flow de tratamento de gps dos ônibus a cada 15 minutos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/135)