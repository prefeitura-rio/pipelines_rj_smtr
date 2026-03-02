# Changelog - treatment

## [1.2.4] - 2025-11-19

### Adicionado

- Adiciona parâmetro `test_webhook_key` na função de criação da materialização genérica (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

## [1.2.3] - 2025-10-05

### Adicionado

- Adiciona parâmetro `skip_if_running_tolerance` na função de criação de flow genérico de materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1029)

## [1.2.2] - 2025-10-02

### Alterado

- Substitui função de leitura do BigQuery da BD pela do pandas_gbq (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/918)

## [1.2.1] - 2025-10-02

### Adicionado

- Adiciona parâmetro `redis_key_suffix` no `DBTSelector` para diferenciar selectors com mesmo nome mas fontes de dados distintas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/916)

## [1.2.0] - 2025-09-29

### Adicionado

- Adiciona parâmetro `fallback_run` no flow genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Cria condicional para execução com base no parâmetro `fallback_run` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Cria task `test_fallback_run` para determinar se a materialização deve ser executada (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [1.1.9] - 2025-08-19

### Alterado

- Altera o `delay_days` em `delay_days_start` e `delay_days_end` na classe `DBTTest` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783)

## [1.1.8] - 2025-07-21

### Corrigido

- Corrige task `run_dbt` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/715)

## [1.1.7] - 2025-07-09

### Adicionado

- Adiciona parâmetro `exclude` no objeto `DBTTest`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)
- Adiciona parâmetro `exclude` no `dbt_pre_test` e `dbt_post_test` do flow genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)

## [1.1.6] - 2025-06-17

### Adicionado

- Cria objeto `DBTTest` para configuração dos testes do dbt (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/622)
- Cria task `setup_dbt_test` que retorna variáveis dos testes do dbt (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/622)
- Adiciona parâmetro `test_scheduled_time` no flow genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/622)

### Corrigido

- Corrige task `check_dbt_test_run` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/622)

## [1.1.5] - 2025-06-06

### Adicionado

- Adiciona lógica para execução dos testes do dbt no flow de materialização genérico (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/614)

## [1.1.4] - 2025-05-22

### Adicionado

- Adiciona parâmetro `additional_vars` na task `create_dbt_run_vars` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/586)

## [1.1.3] - 2025-05-06

### Adicionado

- Adiciona raise `FAIL` na task `dbt_data_quality_checks` se a task `run_dbt_tests` falhar (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/565)

## [1.1.2] - 2025-04-29

### Adicionado

- Adiciona comando `source freshness` da task `run_dbt` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/554)

## [1.1.1] - 2025-04-14

### Adicionado

- Cria task `run_dbt` que permite executar `models`, `snapshots` e `tests`, e adiciona parâmetro `snapshot_selector` no flow de materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/547)

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