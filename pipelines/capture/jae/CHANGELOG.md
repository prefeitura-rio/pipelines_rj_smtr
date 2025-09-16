# Changelog - source_jae

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