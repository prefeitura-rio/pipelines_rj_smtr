# Changelog - projeto_subsidio_sppo

## [1.2.0] - 2025-05-08

### Adicionado

- Adiciona execução de snapshots nos flows `viagens_sppo` e `subsidio_sppo_apuracao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/570)

## [1.1.5] - 2025-03-27

### Corrigido

- Corrigida a materialização do modelo `aux_calendario_manual.sql` no flow `viagens_sppo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/500)

## [1.1.4] - 2025-03-27

### Alterado

- Alterado teste `test_check_gps_treatment.sql` para desconsiderar duplicações assim como o modelo `sppo_aux_registros_filtrada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/500)

## [1.1.3] - 2025-03-21

### Adicionado

- Adicionados os testes da modelo `viagem_planejada.sql` aos testes do subsídio (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/479)

## [1.1.2] - 2025-02-17

### Corrigido

- Corrige a descrição dos testes do modelo `sumario_faixa_servico_dia_pagamento.sql`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/433)

## [1.1.1] - 2025-02-11

### Corrigido

- Corrige logica de execução dos teste no flow `subsidio_sppo_apuracao`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/423)

## [1.1.0] - 2025-02-04

### Adicionado

- Adicionados os testes do modelo `sumario_faixa_servico_dia_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

### Corrigido

- Corrigidas e refatoradas as descrições dos testes do subsídio (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

## [1.0.9] - 2025-01-23

### Alterado

- Remove parâmetro `stu_data_versao` do flow `subsidio_sppo_apuracao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/395)

## [1.0.8] - 2025-01-03

### Corrigido

- Corrigida a materialização dos modelos do dataset `monitoramento` no flow do subsídio (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/391)

## [1.0.7] - 2025-01-03

### Adicionado

- Adiciona a materialização dos modelos do dataset `monitoramento` ao flow do subsídio (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/372)

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