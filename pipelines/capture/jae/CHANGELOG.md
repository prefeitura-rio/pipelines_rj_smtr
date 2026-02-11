# Changelog - source_jae

## [3.1.9] - 2026-02-11

### Adicionado

- Adiciona tabelas `transacao_erro`, `temp_cancelamento_estudante_08122025` e `temp_cancelamento_estudante_sme_08122025` no exclude do backup dos dados da BillingPay

## [3.1.8] - 2026-02-04

### Removido

- Remove schedule do flow `CAPTURA_GPS_VALIDADOR` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1211)

## [3.1.7] - 2026-01-07

### Removido

- Remove schedule do flow `CAPTURA_AUXILIAR` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1155)

## [3.1.6] - 2025-12-15

### Adicionado

- Adiciona tabelas no exclude do backup dos dados da BillingPay (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1111)

### Alterado

- Altera delay de captura da tabela lancamento para 24h (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1111)

## [3.1.5] - 2025-12-08

### Adicionado

- Adiciona tabelas no exclude do backup dos dados da BillingPay (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1087)

### Alterado

- Altera IP do banco tracking_db (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1087)

## [3.1.4] - 2025-11-28

### Alterado

- Altera IP do banco tracking_db (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1074)

## [3.1.3] - 2025-11-27

### Alterado

- Altera IP do banco principal_db (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1071)

## [3.1.2] - 2025-11-10

### Adicionado

- Adiciona verificação da captura das tabelas `gratuidade`, `estudante` e `laudo_pcd` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1037)
- Cria parâmetro `final_timestamp_exclusive` para o teste de captura (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1037)

## [3.1.1] - 2025-11-06

### Corrigido

- Corrige query de captura de gratuidades (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1035)

## [3.1.0] - 2025-11-05

### Adicionado

- Cria possibilidade de verificar as capturas no MySQL e de capturas com intervalos diferentes de 1 minuto (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1029)
- Adiciona verificação da captura da tabela `cliente` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1029)

## [3.0.3] - 2025-10-14

### Alterado

- Altera lógica da task `get_capture_gaps` para executar a contagem de registros do datalake quando a tabela não possui o parametro `primary_keys` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/949)
- Altera primary key da verificação de captura da tabela `lancamento` para `["ifnull(id_lancamento, concat(string(dt_lancamento), '_', id_movimento))","id_conta"]` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/949)

## [3.0.2] - 2025-10-14

### Alterado

- Altera primary key da verificação de captura da tabela `lancamento` para `["ifnull(id_lancamento, string(dt_lancamento))", "id_conta"]` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/948)

### Corrigido

- Corrige timezone da coluna data da tabela `resultado_verificacao_captura_jae` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/948)

## [3.0.1] - 2025-10-02

### Alterado
- Substitui função de leitura do BigQuery da BD pela do pandas_gbq (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/918)

## [3.0.0] - 2025-09-29

### Adicionado
- Adiciona schedules adicionais para fallback nos flows `CAPTURA_TRANSACAO_ORDEM` e `CAPTURA_INTEGRACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Adiciona função `raise_if_column_isna(column_name="id_ordem_pagamento")` no pretratamento da captura das tabelas `ordem_rateio` e `ordem_ressarcimento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

### Alterado
- Altera captura das tabelas do ressarcimento_db e da transacao_ordem de 8:00 para 10:00 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)
- Altera timedelta do schedule do backup dos bancos mais pesados (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [2.5.0] - 2025-09-16

### Adicionado
- Cria captura das tabelas `escola`, `estudante` e `laudo_pcd` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875)

### Alterado
- Altera query da captura da tabela `gratuidade`, removendo os joins com as tabelas `escola`, `estudante` e `laudo_pcd` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875)

## [2.4.0] - 2025-09-08

### Adicionado
- Salva resultados da verificação da captura do flow `verifica_captura` na tabela `source_jae.resultado_verificacao_captura_jae` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/848)
- Adiciona checagem de captura da tabela `lancamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/848)

### Alterado
- Muda lógica do delay na captura da tabela `lancamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/848)
- Muda logica das primary keys no flow `verifica_captura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/848)

## [2.3.6] - 2025-08-22

### Alterado
- Aumenta memória nos flows `CAPTURA_INTEGRACAO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/807)

## [2.3.5] - 2025-08-11

### Alterado

- Altera schedules das capturas das ordens de pagamento e da transacao_ordem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/781)

## [2.3.4] - 2025-08-05

### Alterado
- Aumenta memória nos flows `CAPTURA_INTEGRACAO`, `CAPTURA_TRANSACAO_ORDEM` e `verifica_captura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/759)

## [2.3.3] - 2025-08-04

### Removido

- Remove filtro da captura da tabela linha (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/755)


## [2.3.2] - 2025-07-31

### Alterado

- Altera schedules das capturas das ordens de pagamento e da transacao_ordem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/743)
- Altera chunk_size da captura da transacao_ordem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/743)

## [2.3.1] - 2025-07-30

### Alterado

- Altera schedules das capturas das ordens de pagamento e da transacao_ordem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/740)

## [2.3.0] - 2025-07-29

### Adicionado

- Cria flow `CAPTURA_LANCAMENTO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/735)

## [2.2.0] - 2025-07-28

### Adicionado

- Cria flow `verifica_captura` para checagem de completude dos dados capturados em relação ao DB da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/733)

### Alterado

- Altera lógica de definição do delay de captura dos dados da Jaé, para criar compatibilidade entre versões (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/733)

## [2.1.6] - 2025-07-09

### Alterado

- Altera IP do banco `principal_db` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/679)

## [2.1.5] - 2025-07-07

### Alterado

- Refatora captura da tabela `integracao_transacao` para utilizar paginação (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/664)

## [2.1.4] - 2025-07-03

### Adicionado

- Cria flow `CAPTURA_TRANSACAO_RETIFICADA` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/658)

## [2.0.4] - 2025-07-02

### Alterado
- Altera IP do banco `principal_db` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/655)
- Adiciona tabelas `estudante_seeduc_25032025`, `estudante_24062025`, `estudante_20062025` e `producao_20250617081705_02_VT` no exclude do backup da billing pay (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/655)

### Alterado

## [2.0.3] - 2025-04-28

### Alterado
- Altera politica de retry da get_raw (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/555)

## [2.0.2] - 2025-04-25

### Alterado
- Altera IPs dos bancos da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/552)

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