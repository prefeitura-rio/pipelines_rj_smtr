# Changelog - gtfs

## Corrigido

## [1.0.5] - 2024-07-23

- Corrigido o parse da data_versao_gtf (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/118)

## [1.0.4] - 2024-07-17

### Adicionado

- Adiciona parametros para a captura manual do gtfs no flow `SMTR: GTFS - Captura/Tratamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/82/)

## [1.0.3] - 2024-07-04

## Corrigido

- Corrigido o formato da data salva no redis de d/m/y para y-m-d (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/91)


## [1.0.2] - 2024-06-21

### Adicionado

- Adiciona DocString nas funções de `utils.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/74)
- Adicionado log da linha selecionada para captura na task `get_os_info` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/74)

## [1.0.1] - 2024-06-20

### Corrigido

- Corrige task `get_last_capture_os` para selecionar a key salva no dicionário do Redis (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/72)

## [1.0.0] - 2024-06-19

### Adicionado

- Adicionada automação da captura do gtfs atravez da planilha Controle OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/62)


