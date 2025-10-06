# Changelog - capture

## [1.2.0] - 2025-09-29

### Adicionado

- Cria novo parâmetro `recapture_timestamps` no flow genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

### Alterado

- Altera task `get_capture_timestamps` para utilizar o novo parâmetro `recapture_timestamps` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [1.1.3] - 2025-07-07

### Alterado

- Refatora flow de captura genérico e as tasks `get_raw_data`, `upload_raw_file_to_gcs` e `transform_raw_to_nested_structure` para suportar captura por chunks (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/664)

## [1.1.2] - 2025-05-09

### Alterado

- Altera task `transform_raw_to_nested_structure` para adicionar coluna `_datetime_execucao_flow` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/571)

## [1.1.1] - 2025-04-28

### Adicionado
- Adiciona parâmetro para ajustar quantidade de retries na task `get_raw` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/555)

## [1.1.0] - 2025-03-26

### Alterado

- Altera lógica do parâmetro `source` na função `create_default_capture_flow` para utilizar o mesmo flow para capturar vários sources (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/489)
- Altera função `create_default_capture_flow` para agendar runs de recaptura automaticamente pelo parâmetro `recapture_schedule_cron` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/489)

## [1.0.1] - 2024-11-25

### Alterado

- Altera função de pre-tratamento caso o número de primary_keys for igual ao de colunas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/333)

## [1.0.0] - 2024-10-21

### Adicionado

- Cria flow de captura genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/276)