# Changelog - bilhetagem_processos_manuais

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