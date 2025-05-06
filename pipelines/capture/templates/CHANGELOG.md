# Changelog - capture

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