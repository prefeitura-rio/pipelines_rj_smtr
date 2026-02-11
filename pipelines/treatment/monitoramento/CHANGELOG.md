# Changelog - monitoramento

## [1.3.1] - 2026-02-11

### Adicionado

- Adiciona parâmetro `tipo_materializacao` no flow `MONITORAMENTO_TEMPERATURA_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1236)

## [1.2.11] - 2025-11-19

### Adicionado

- Adiciona parâmetro `test_webhook_key` no flow de materialização da `gps_validador` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

## [1.2.10] - 2025-11-05

- Adiciona tolerância no handler skip if running da materialização do `gps_validador` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)

## [1.2.9] - 2025-10-02

### Adicionado

- Cria selector para cada fonte de GPS (conecta, cittati, zirix) para os modelos `gps` e `gps_15_minutos` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/916)

## [1.2.8] - 2025-09-02

### Adicionado

- Cria flow de materialização `monitoramento_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)

### Alterado

- Altera constantes de monitoramento de veículos removendo testes relacionados a climatização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)

## [1.2.7] - 2025-09-01

### Adicionado

- Move materialização do `gps_validador` para o dataset `monitoramento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [1.2.6] - 2025-08-05

### Adicionado

- Adiciona descrição do teste `test_completude__temperatura_inmet` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/761)

## [1.2.5] - 2025-07-28

### Adicionado

- Adiciona descrição dos testes de unicidade no `MONITORAMENTO_VEICULO_CHECKS_LIST` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/708)

## [1.2.4] - 2025-07-23

### Alterado

- Alterado o flow `MONITORAMENTO_VEICULO_MATERIALIZACAO` para adicionar a materialização do snapshot (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/720)

## [1.2.3] - 2025-07-09

### Corrigido

- Corrige nome do teste na `VEICULO_DIA_CHECKS_LIST` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)

### Adicionado

- Adiciona parâmetro exclude no `MONITORAMENTO_VEICULO_TEST` para não executar o teste `test_check_veiculo_lacre__veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)

## [1.2.2] - 2025-07-08

### Adicionado

- Adiciona novos testes em  `MONITORAMENTO_VEICULO_CHECKS_LIST` e `VEICULO_DIA_CHECKS_LIST` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/668)

## [1.2.1] - 2025-06-30

### Alterado

- Altera agendamento do flow `MONITORAMENTO_VEICULO_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/653)

## [1.2.0] - 2025-06-25

### Adicionado

- Adiciona execução de testes do DBT no flow `MONITORAMENTO_VEICULO_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)
- Cria flow `VEICULO_DIA_MATERIALIZACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [1.1.6] - 2025-06-17

### Alterado

- Altera parâmetros para execução do teste do dbt no flow de materialização do gps (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/622)

## [1.1.5] - 2025-06-06

### Adicionado

- Adiciona parâmetros para execução do teste do dbt no flow de materialização do gps (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/614)

## [1.1.4] - 2025-05-28

### Adicionado

- Cria flow de materialização do gps e gps 15 minutos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/586)

## [1.1.3] - 2025-05-27

### Adicionado

- Cria flow de materialização do monitoramento dos veiculos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/594)

## [1.1.2] - 2024-11-28

### Alterado

- Altera hora de execução da validação de viagens e da materialização das viagens informadas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)

### Corrigido

- Ajusta espera pelos dados de viagem informada na materialização da validação (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)

## [1.1.1] - 2024-11-25

### Alterado

- Substitui variavel de expressão cron pela função `create_daily_cron` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/333)

## [1.1.0] - 2024-11-08

### Adicionado

- Cria flow de tratamento de validação de viagens informadas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

## [1.0.0] - 2024-10-21

### Adicionado

- Cria flow de tratamento de viagens informadas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/276)
