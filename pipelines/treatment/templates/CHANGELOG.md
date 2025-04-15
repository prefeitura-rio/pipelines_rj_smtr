# Changelog - treatment

## [1.1.1] - 2025-04-14

### Adicionado

- Adiciona task `run_dbt_snapshot` e parâmetros `run_snapshot` e `snapshot_selector` no flow de materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/547)

## [1.1.0] - 2025-03-13

### Adicionado

- Adiciona parametros `raise_check_error` e `additional_mentions` na task `dbt_data_quality_checks` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/474)

### Corrigido

- Altera lógica de construção do path na função `parse_dbt_test_output` para funcionar em qualquer sistema (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/474)
- Corrige select do teste DBT pelo nome na task `check_dbt_test_run` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/474)

## [1.0.4] - 2024-12-17

### Adicionado

- Adiciona raise FAIL caso os testes do DBT falhem na task `dbt_data_quality_checks` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/256)
- Adiciona parametro `webhook_key` na task `dbt_data_quality_checks` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/256)

## [1.0.3] - 2024-12-13

### Adicionado

- Adiciona trigger `all_finished` na task `dbt_data_quality_checks` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)
- Adiciona task `log_discord` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)

### Alterado

- Refatora task `dbt_data_quality_checks` para informar o nome do teste quando a descrição não estiver definida no arquivo `constants.py` e garantir compatibilidade com diferentes parâmetros de data dos modelos DBT (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)

### Corrigido

- Corrige problema de construção incorreta de caminhos ao acessar arquivos na função `parse_dbt_test_output` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)

## [1.0.2] - 2024-11-08

### Alterado

- Adiciona lógica para verificar fontes de dados no padrão antigo (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

## [1.0.1] - 2024-10-29

### Adicionado

- Adiciona as tasks `check_dbt_test_run`, `run_dbt_tests` e `dbt_data_quality_checks` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/288)
- Adiciona a função `parse_dbt_test_output` no `utils.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/288)


## [1.0.0] - 2024-10-21

### Adicionado

- Cria flow de materialização genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/276)