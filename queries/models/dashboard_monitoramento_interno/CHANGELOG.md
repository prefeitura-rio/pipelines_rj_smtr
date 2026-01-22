# Changelog - dashboard_monitoramento_interno

## [1.0.2] - 2026-01-21

### Alterado

- Altera o modelo `view_viagem_climatizacao.sql` para consultar os dados de `id_validador` e `operadora` da tabela `validador_operadora` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1181)

## [1.0.1] - 2026-01-08

### Alterado

- Altera o nome no `schema` para `view_viagem_climatizacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1157)
- Ajusta a data para incluir a variável `("{{ var('start_date') }}")` e `("{{ var('end_date') }}")` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1157)

## [1.0.0] - 2026-01-06

### Alterado

- Move view `viagem_climatizacao.sql` de `dashboard_subsidio_sppo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1121)