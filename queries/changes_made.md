# Fusion Migration: Changes Made

## Summary

Migration of the `rj_smtr` dbt project to the dbt Fusion engine (dbt-fusion 2.0.0-preview.104).

- **`dbt parse`**: Completed successfully with **0 errors, 0 warnings**
- **`dbt compile`**: 69 remaining errors — all are **infrastructure/data issues**, not Fusion compatibility issues (see "Remaining Issues" below)

## Changes Made

### 1. Behavior flag set by dbt-autofix

- **File**: `dbt_project.yml`
- **Error**: Deprecation — `require_generic_test_arguments_property` not set
- **Fix**: dbt-autofix set the flag `require_generic_test_arguments_property: True`
- **Why**: Required by the new authoring layer to properly parse generic test arguments

### 2. Moved `deprecation_date` out of version `config:` blocks (5 errors fixed)

- **File**: `models/projeto_subsidio_sppo_encontro_contas/schema.yml`
- **Error**: `dbt1060: While parsing version config: Ignored unexpected key "deprecation_date"`
- **Fix**: Moved `deprecation_date` from inside the `config:` block to the version level for 5 model versions (`balanco_consorcio_ano` v1, `balanco_consorcio_dia` v1, `balanco_servico_ano` v1, `balanco_servico_dia` v1, `balanco_servico_quinzena` v1)
- **Why**: `deprecation_date` is a version-level property in dbt, not a config property. Fusion's stricter parsing rejects it inside `config:`.

### 3. Fixed `adapter.get_relation()` with None database in test macro (1 error fixed)

- **File**: `macros/test_sincronizacao_tabelas.sql`
- **Error**: `dbt1501: Failed to render SQL — argument 'database' to get_relation() has incompatible type None`
- **Fix**: Wrapped all adapter-dependent logic (`adapter.get_relation()`, `adapter.get_columns_in_relation()`, and the SQL generation) inside an `{% if execute %}` guard, with a placeholder `select 1 as placeholder where false` for parse time. Also replaced the regex-based extraction of database/schema/table with direct `model.database`, `model.schema`, `model.identifier` properties.
- **Why**: During Fusion's parse phase, `model.database` is `None` and `adapter.get_relation()` requires a non-null string. The `{% if execute %}` guard ensures adapter calls only happen at runtime.

### 4. Replaced `re.finditer()` with `re.findall()` in macro (17 errors fixed)

- **File**: `macros/custom_get_where_subquery.sql`
- **Error**: `dbt1501: Failed to render SQL — unknown method: map has no method named finditer`
- **Fix**: Replaced `modules.re.finditer("\{([^}]+)\}", where)` with `modules.re.findall("\\{([^}]+)\\}", where)` and updated the loop to iterate over the returned list of captured group strings directly (removing `match.group(1)` calls).
- **Why**: Fusion's Jinja engine does not support `re.finditer()`. `re.findall()` returns captured groups directly and achieves the same result.

### 5. Removed empty source definitions (2 warnings fixed)

- **File**: `models/br_rj_riodejaneiro_recursos/schema.yml`
  - **Warning**: `dbt0102: No tables defined for source 'br_rj_riodejaneiro_recursos'`
  - **Fix**: Removed the empty `sources: - name: br_rj_riodejaneiro_recursos` block (no tables, no config, not referenced via `source()`)
- **File**: `models/gtfs/schema.yml`
  - **Warning**: `dbt0102: No tables defined for source 'gtfs'`
  - **Fix**: Removed the empty `sources: - name: "gtfs"` block (duplicate — the real `gtfs` source with tables is defined in `models/sources.yml`)
- **Why**: Fusion requires sources to have at least one table defined. Both were empty/duplicate definitions.

### 6. Regenerated `package-lock.yml` (1 warning fixed)

- **Warning**: `dbt1041: Old format package-lock.yml file found`
- **Fix**: Deleted old `package-lock.yml` and `dbt_packages/`, then ran `dbt deps` to regenerate both with the current format.
- **Why**: Fusion requires the new `package-lock.yml` format.

### 7. Fixed missing `from` keyword in SQL (1 error fixed)

- **File**: `models/br_rj_riodejaneiro_rdo_staging/rdo_servico_dia.sql` (line 77)
- **Error**: `dbt0101: mismatched input '`rj-smtr`' expecting one of ',', 'EXCEPT', 'FROM', ...`
- **Fix**: Added the missing `from` keyword before `{{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }}`
- **Why**: Pre-existing SQL bug — the `FROM` keyword was missing between the SELECT column list and the table reference. Fusion's static SQL analysis caught this.

### 8. Fixed renamed column `timestamp_gps` → `datetime_gps` (1 error fixed)

- **File**: `models/monitoramento/gps_15_minutos_union.sql`
- **Error**: `dbt0227: No column timestamp_gps found`
- **Fix**: Changed all 3 occurrences of `timestamp_gps` to `datetime_gps` in the SELECT statements
- **Why**: The source table `gps_15_minutos_onibus_conecta` column was renamed from `timestamp_gps` to `datetime_gps`. Fusion's static analysis detected the mismatch.

### 9. Disabled unused dbt example models (4 errors fixed)

- **Files**: `models/example/my_first_dbt_model.sql`, `models/example/my_second_dbt_model.sql`
- **Error**: `dbt0227: No column data found` (from global test `+where` clause referencing `data` column)
- **Fix**: Added `{{ config(enabled=false) }}` to both starter template models
- **Why**: These are dbt starter templates with only an `id` column. The project's global test `where` clause references a `data` column that doesn't exist in these models.

## Remaining Issues (Not Fusion Compatibility)

These 69 compile errors are **not** caused by Fusion incompatibility — they are infrastructure, permissions, or pre-existing data issues:

### BigQuery Storage API errors (62 errors)

- **Error**: `dbt1308: [BigQuery] Storage API is not available for query`
- **Cause**: The BigQuery Storage Read API is not enabled or the current credentials lack permission to use it. Fusion uses this API during compilation for static analysis of remote table schemas.
- **Fix**: Enable the BigQuery Storage Read API in your GCP project (`rj-smtr`) and ensure the service account or OAuth user has the `bigquery.readSession.create` permission (role `roles/bigquery.readSessionUser`).

### Missing remote tables (6 errors)

- `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_agg_data_hora_linha` (4 references)
- `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_logs` (1 reference)
- `rj-smtr.brt_manutencao.questionario` (1 reference)
- **Cause**: These tables are referenced by models but don't exist in BigQuery.

### Snapshot column mismatch (1 error)

- **File**: `snapshots/dados_mestres/snapshot_tuneis_logradouro.sql`
- **Error**: Column `datetime_ultima_atualizacao` not found in `datario.dados_mestres.logradouro`
- **Cause**: The external source table's column appears to have been renamed to `data_ultima_edicao`.

### UNION ALL column count mismatch (1 error)

- **File**: `models/cadastro/staging/staging_linha.sql`
- **Error**: `dbt0301: Expected 4 columns, but got 5`
- **Cause**: `source("source_jae", "linha")` and the hardcoded `rj-smtr-dev.source_jae.linha` table have different numbers of columns. This is a schema drift between environments.
