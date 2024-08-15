# Changelog - projeto_subsidio_sppo

## [1.0.3] - 2024-08-08

### Adicionado

- Adiciona constante `SUBSIDIO_SPPO_V2_DATASET_ID` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/137)

### Alterado

- Alterado o flow `subsidio_sppo_apuracao` para utlizar a constante `SUBSIDIO_SPPO_V2_DATASET_ID` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/137)

## [1.0.2] - 2024-07-31

### Adicionado

- Adiciona `km_apurada_sem_transacao` na soma da constante `SUBSIDIO_SPPO_DATA_CHECKS_PARAMS.teste_sumario_servico_dia_tipo_soma_km`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)

## [1.0.1] - 2024-07-17

### Alterado

- Alterado o schedule do flow `SMTR: Subsídio SPPO Apuração - Tratamento` de 07:00 para 07:05 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/106)

## [1.0.0] - 2024-06-28

### Alterado

- Alterada a tabela `registros` para `sppo_registros` nas constantes `SUBSIDIO_SPPO_DATA_CHECKS_PARAMS.check_gps_treatment` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/84)