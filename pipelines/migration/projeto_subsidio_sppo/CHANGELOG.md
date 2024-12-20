# Changelog - projeto_subsidio_sppo

## [1.0.6] - 2024-12-17

### Adicionado

- Adiciona automação dos testes do DBT no arquivo `flows.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/256)

## [1.0.5] - 2024-08-29

### Alterado

- Alterado `teste_sumario_servico_dia_tipo_soma_km` para considerar tabela de acordo com o período (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

- Alterado `indicador_viagem_remunerada` para `indicador_viagem_dentro_limite` no `SUBSIDIO_SPPO_DATA_CHECKS_POS_LIST` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

- Alterada a lógica do flow `subsidio_sppo_apuracao` para utilizar os selectors `apuracao_subsidio_v8` e `apuracao_subsidio_v9` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

- Alterada a lógica da task `subsidio_data_quality_check` para considerar tabela de acordo com o período (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

### Adicionado

- Adiciona parâmetro run_d0 para materializar D+0 as tabelas `viagem_planejada` e `subsidio_data_versao_efetiva` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

## [1.0.4] - 2024-08-19

### Adicionado

- Adicionados os testes `Todas as viagens foram processadas com feed atualizado do GTFS` e `Todas as viagens foram atualizadas antes do processamento do subsídio` na constante `SUBSIDIO_SPPO_DATA_CHECKS_POS_LIST` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/147)

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