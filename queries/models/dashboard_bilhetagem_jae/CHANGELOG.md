# Changelog - dashboard_bilhetagem_jae

## [1.0.2] - 2024-08-05

### Adicionado
- Adiciona filtro para remover dados de transações do tipo RioCard no modelo `view_passageiros_hora.sql` e `view_passageiros_tile_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

## [1.0.1] - 2024-07-17

### Corrigido
- Deduplica ids dos serviços (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/109)

## [1.0.0] - 2024-06-11

### Adicionado
- Cria modelo `view_passageiros_tile_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)
- Cria tratamento da coluna `tipo_transacao_smtr` nos modelos `view_passageiros_tile_hora.sql` e `view_passageiros_hora.sql` para alterar o tipo `Integral` para `Tarifa Integral` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)