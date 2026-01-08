# Changelog - source_serpro

## [1.0.2] - 2025-12-26

### Alterado

- Desativa schedule do flow `CAPTURA_SERPRO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1135)

## [1.0.1] - 2025-04-15

### Alterado

- Ativa schedule diário do flow `CAPTURA_SERPRO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/525)
- Altera filtro da query para capturar pela `data_atualizacao_dl` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/525)
- Altera a task `create_serpro_extractor` para utilizar a função `extract_serpro_data` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/525)

### Adicionado

- Cria as funções `extract_serpro_data`, `connect_and_execute` e `query_result_to_csv` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/525)

## [1.0.0] - 2025-04-04

### Adicionado

- Cria flow de captura de autuações SERPRO (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/513)