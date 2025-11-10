# Changelog - bilhetagem_processos_manuais

## [1.2.3] - 2025-11-10

### Corrigido

- Corrige parâmetro `table_id` na recaptura de gaps (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1037)
- Reprocessa dados até a data atual no caso de timestamp divergente de clientes (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1037)

## [1.2.2] - 2025-11-05

### Adicionado

- Adiciona recaptura da tabela `cliente` no flow `timestamp_divergente_jae_recaptura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1029)

## [1.2.1] - 2025-10-21

### Adicionado

- Adiciona recaptura de integrações no flow `ordem_atrasada`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/959)

## [1.2.0] - 2025-10-03

### Adicionado

- Adiciona tabela extrato na rematerialização de gaps (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/924)

### Alterado

- Faz a recaptura de gaps em chunks de 20 timestamps (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/924)

## [1.0.1] - 2025-10-02

### Adicionado

- Adiciona recaptura das ordens no flow de ordens atrasadas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/918)

## [1.0.0] - 2025-09-29

### Adicionado

- Cria flows `ordem_atrasada` e `timestamp_divergente_jae_recaptura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)