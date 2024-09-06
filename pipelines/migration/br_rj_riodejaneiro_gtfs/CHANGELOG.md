# Changelog - gtfs

## [1.0.7] - 2024-09-06

### Adicionado

- Cria função `processa_ordem_servico_faixa_horaria` e adiciona chamada na task `get_raw_drive_files` para processar o anexo da faixa horária (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/193)

- Adiciona materialização do modelo `ordem_servico_faixa_horaria` no flow (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/193)

## [1.0.7] - 2024-08-27

### Alterado

- Alterada a forma de identificação das tabs das planilhas de Ordem de servico e Trajetos alternativos para identificar atravez dos sufixos `ANEXO I` e `ANEXO II` respectivamente (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/171)

## [1.0.6] - 2024-08-02

### Adicionado

- Adiciona filtro para os nomes de tabs da planilha de controle os na task `get_raw_drive_files` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/128/files)

- Adiciona etapa de remover pontos antes da converção de metro para km no processamento da OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/129)

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


