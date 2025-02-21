# Changelog - onibus_gps_zirix

## [1.0.5] - 2025-02-21

### Alterado
- Torna filtro de partição obrigatório nos modelos `sppo_aux_registros_filtrada_zirix.sql` e `gps_sppo_zirix.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/448)

## [1.0.4] - 2024-08-09

### Adicionado
- Adiciona descrição em todas as colunas do modelo `gps_sppo_zirix.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)

## [1.0.3] - 2024-08-02

### Alterado
- Adiciona tag `geolocalizacao` ao modelo `gps_sppo_zirix.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)

## [1.0.2] - 2024-07-02

### Adicionado
- Adiciona descrições do modelo `gps_sppo_zirix` no `schema.yml` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/85)

## [1.0.1] - 2024-04-15

### Alterado

- Converte colunas de data e hora do modelo `sppo_realocacao_zirix` para o tipo adequado (https://github.com/prefeitura-rio/queries-rj-smtr/pull/268)

## [1.0.0] - 2024-04-11

### Adicionado

- Criação de copia da `onibus_gps` e `gps_sppo` para a API da zirix