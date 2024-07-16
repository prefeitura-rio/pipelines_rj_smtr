# Changelog - indicadores_continuados_egp

## [1.0.0] - 2024-07-16

### Alterado

- Alterada estrutrutura do modelo, movendo arquivos de staging
- Alterado parâmetro da função `CURRENT_DATE()` para `CURRENT_DATE("America/Sao_Paulo")`, de forma a garantir a data correta
- Alterado modelos `passageiro_gratuidade.sql` e `passageiro_pagante.sql` em razão da nova coluna `modo` no modelo `consorcios.sql`