# Changelog - cadastro_interno

## [1.2.1] - 2025-09-11

### Adicionado

- Adiciona coluna `cpf` no modelo `endereco_cliente_cpf_jae.sql`

### Alterado

- Renomeia coluna `documento` para `cpf` no modelo `cliente_cpf_jae.sql`

### Removido

- Remove coluna `nome_social` no modelo `cliente_cpf_jae.sql`

### Corrigido

- Muda strings vazias para null na coluna `numero` do modelo `endereco_cliente_jae.sql`

## [1.2.0] - 2025-09-08

### Adicionado

- Cria modelo `endereco_cliente_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/854)

## [1.0.0] - 2025-08-27

### Adicionado

- Cria modelo `cliente_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/812)