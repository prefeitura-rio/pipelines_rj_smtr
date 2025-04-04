# Changelog - source_jae

## [2.0.1] - 2025-04-04

### Corrigido

- Ajusta query de captura de gratuidades para evitar duplicação (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/517)

## [2.0.0] - 2025-03-26

### Adicionado

- Migra flows de captura dos dados da jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/489)

## [1.4.1] - 2025-03-19

### Alterado

- Remove tabelas sem permissão automaticamente no backup da BillingPay (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/487)

## [1.4.0] - 2025-03-17

### Adicionado

- Adiciona parâmetro `end_datetime` no flow `backup_billingpay`

## [1.3.2] - 2025-03-17

### Alterado

- Desativa schedule do flow `backup_billingpay_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/482)

## [1.3.1] - 2025-03-10

### Alterado

- Altera IP dos bancos principal_db, gratuidade_db e midia_db da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/472)

## [1.3.0] - 2025-02-25

### Adicionado

- Cria flow `backup_billingpay_historico` para captura dos dados históricos das tabelas grandes (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/455)

## [1.2.1] - 2025-02-20

### Alterado

- Adiciona parâmetros para captura de todos dos bancos da BillingPay, exceto processador_transacao_db (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/439)

## [1.2.0] - 2025-02-11

### Adicionado

- Cria flow `backup_billingpay` para fazer o backup diário dos dados da Jaé dos bancos da BillingPay (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/422)

## [1.1.0] - 2024-12-19

### Adicionado

- Cria flow `verificacao_ip` para alertar sobre falhas de conexão com o banco de dados da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/366)

## [1.0.1] - 2024-12-16

### Alterado
- Altera IP do banco de tracking da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/362)

## [1.0.0] - 2024-11-25

### Adicionado

- Cria flow de captura da relação entre transação e ordem de pagamento