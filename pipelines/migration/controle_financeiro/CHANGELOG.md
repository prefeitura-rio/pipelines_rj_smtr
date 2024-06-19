# Changelog - controle_financeiro

## [1.2.0] - 2024-06-13

### Alterado

- Altera captura do `arquivo_retorno_captura` para usar a dataVencimento como filtro (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)

## [1.1.1] - 2024-06-03

### Corrigido

- Altera label de DEV para PROD no flow `arquivo_retorno_captura` (https://github.com/prefeitura-rio/pipelines/pull/697)

## [1.1.0] - 2024-05-28

### Adicionado

- Cria flow de captura do arquivo de retorno da Caixa, enviado via api pela CCT (https://github.com/prefeitura-rio/pipelines/pull/691)

### Alterado

- Move as constantes dos flows `controle_cct_cb_captura` e `controle_cct_cett_captura` para o arquivo de constantes dentro da pasta do dataset (https://github.com/prefeitura-rio/pipelines/pull/691)

## [1.0.0] - 2024-05-22

### Adicionado

- Criados flows de captura das planilhas de controle financeiro `cb` e `cett` (https://github.com/prefeitura-rio/pipelines/pull/688)