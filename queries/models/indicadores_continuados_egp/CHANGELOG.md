# Changelog - indicadores_continuados_egp

## [1.0.1] - 2025-02-09

### Corrigido

- Corrigida a fonte dos dados de licenciamento no modelo `idade_media_frota_operante_onibus.sql`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1214)

## [1.0.0] - 2024-07-16

### Alterado

- Alterada estrutura do modelo, movendo arquivos de `staging` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/104)
- Alterado parâmetro da função `CURRENT_DATE()` para `CURRENT_DATE("America/Sao_Paulo")`, de forma a garantir a data correta (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/104)
- Alterados modelos `passageiro_gratuidade.sql` e `passageiro_pagante.sql` em razão da nova coluna `modo` no modelo `consorcios.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/104)
- Alterado `schema.yml` para incluir descrição do modelo `indicadores_mes_pivot.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/104)