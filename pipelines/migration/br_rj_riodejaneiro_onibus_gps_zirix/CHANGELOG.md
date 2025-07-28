# Changelog - br_rj_riodejaneiro_onibus_gps_zirix

## [1.1.1] - 2025-07-21

### Alterado

- Altera a task `run_dbt_model` pela task genérica `run_dbt` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/715)

## [1.1.0] - 2024-11-12

### Adicionado
- Cria flow de recaptura da realocação `recaptura_realocacao_sppo_zirix` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/316)

### Alterado
- Cria lógica de rematerialização do gps_sppo_zirix (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/316)

## [1.0.0] - 2024-09-02

### Corrigido
- Corrige table_id na task `get_materialization_date_range` no flow `materialize_sppo_zirix` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/181)
