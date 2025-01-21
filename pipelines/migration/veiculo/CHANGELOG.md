# Changelog - veiculo

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