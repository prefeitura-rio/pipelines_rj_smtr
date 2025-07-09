# Changelog - monitoramento

## [1.2.3] - 2025-07-09

### Corrigido

- Corrige nome do teste na `VEICULO_DIA_CHECKS_LIST` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)

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