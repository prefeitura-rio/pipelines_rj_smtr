# Changelog - dashboard_operacao_onibus

## [1.0.4] - 2025-09-18

### Corrigido

- Corrige a coluna `sentido` no modelo `aux_ordem_servico_diaria_v4` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/880)

## [1.0.3] - 2025-09-16

### Adicionado

- Cria modelo `aux_ordem_servico_diaria_v4` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/873)

### Alterado

- Altera modelo `ordem_servico_diaria` para incluir os dados mais recentes (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/873)

## [1.0.2] - 2025-06-10

### Adicionado

- Cria modelo `aux_ordem_servico_diaria_v3` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/620)

### Alterado

- Altera modelo `ordem_servico_diaria` para incluir os dados mais recentes (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/620)

## [1.0.1] - 2025-04-30

### Alterado

- Refatora `aux_ordem_servico_diaria_v1`, `aux_ordem_servico_diaria_v2` e `servicos_sentido` para remover dependência do `ordem_servico_gtfs` (desativado) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

## [1.0.0] - 2025-01-16

### Adicionado

- Cria novo dataset para views utilizadas no dashboard de operação dos ônibus (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/389)