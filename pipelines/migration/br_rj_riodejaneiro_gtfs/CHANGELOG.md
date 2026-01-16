# Changelog - gtfs

## [1.3.0] - 2026-01-16

### Adicionado
- Adicionado teste no modelo `trips_gtfs`para verificar se existe o `shape_id` no modelo `shapes_gtfs` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1174)

## [1.2.9] - 2025-12-10

### Alterado

- Alterado o flow `gtfs_captura_nova` para a captura da `ordem_servico_trajeto_alternativo_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1050)

## [1.2.8] - 2025-10-20

### Alterado

- Alterado o flow `gtfs_captura_nova` para a geração da tabela externa `ordem_servico_trajeto_alternativo_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/961)

## [1.2.7] - 2025-10-14

### Alterado

- Altera `get_raw_gtfs_files` para usar a função `get_google_api_service` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/943)
- Altera tratamento das colunas `quilometragem`,`viagens` e `partidas` para substituir `-` por `0` na `processa_ordem_servico_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/943)

## [1.2.6] - 2025-10-06

### Corrigido

- Corrige checagem de falha da materialização do GTFS no flow `gtfs_captura_nova` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/922)

## [1.2.5] - 2025-07-21

### Alterado

- Altera as tasks `run_dbt_model` e `run_dbt_tests` pela task genérica `run_dbt` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/715)

## [1.2.4] - 2025-07-16

### Alterado
- Altera o flow `gtfs_captura_nova` para o novo modelo de OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/704)

## [1.2.3] - 2025-05-06

### Corrigido
- Remove testes de tecnologia (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/565)

## [1.2.2] - 2025-04-30

### Alterado

- Refatora flow, task `get_raw_gtfs_files` e funções `processa_ordem_servico`, `processa_ordem_servico_trajeto_alternativo` e `processa_ordem_servico_faixa_horaria` para subir automaticamente diferentes tipos de OS e novo modelo de OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

## [1.2.1] - 2025-01-13

### Corrigido
- Removido teste de Ordem de Serviço regular (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/386)

## [1.2.0] - 2025-01-03

### Adicionado
- Adicionado schedule de 5 minutos do flow de captura do gtfs (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/379)

- Adicionado parâmetros personalizados de execução no arquivo `flows.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/379)

### Removido
- Removido o teste de quantidade de abas na planilha da Ordem de Serviço (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/379)

## [1.1.9] - 2025-01-02

### Alterado
- Remove teste de verificação de quilometragem da task `processa_ordem_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/377)

## [1.1.8] - 2024-12-30

### Alterado
- Exclui modelo `matriz_integracao` da materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)

## [1.1.7] - 2024-12-13

### Adicionado

- Cria arquivo `constants.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)
- Adiciona automação dos testes do DBT no arquivo `flows.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)
- Adiciona compatibilidade com padrão "KM" na função `processa_ordem_servico` no arquivo `utils.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)

### Alterado

- Remove parâmetros personalizados de execução no arquivo `flows.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)
- Troca task `transform_raw_to_nested_structure` pela `transform_raw_to_nested_structure_chunked` no arquivo `flows.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)

### Corrigido

- Corrige parâmetro `supportsAllDrives` na função `download_xlsx` arquivo `utils.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/359)

## [1.1.6] - 2024-12-04

- Adiciona o modelo `viagem_planejada_planejamento` no exclude da materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/352)

## [1.1.5] - 2024-12-03

### Corrigido

- Corrige a conversão de valores para float na OS por faixa horaria (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/350)

- Corrige a captura dos arquivos do GTFS no drive compartilhado (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/350)

## [1.1.4] - 2024-11-07

### Alterado

- Refatora função `processa_ordem_servico_faixa_horaria` no arquivo `utils.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/306)

## [1.1.3] - 2024-10-30

### Alterado

- Alterado arquivo `utils.py` em razão das novas faixas horárias (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/295)

## [1.1.2] - 2024-10-21

### Alterado

- Alterado o link da planilha `Control OS` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/278)

## [1.1.1] - 2024-09-13

### Alterado

- Inserido ajuste para o tipo_os `CNU` com feed_start_date `2024-08-16` considerar o planejamento do GTFS de sábado no domingo. Afetado o modelo `ordem_servico_trips_shapes_gtfs.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/224)
- Ajustado tratamento dos modelos `ordem_servico_sentido_atualizado_aux_gtfs.sql` e `ordem_servico_trips_shapes_gtfs.sql` em razão da apuração por faixa horária `DATA_SUBSIDIO_V9_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/224)

## [1.1.0] - 2024-09-11

### Alterado

- Criada feature para subida manual com base nos arquivos no GCS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/206)
- Função `get_raw_drive_files` transformada em `get_raw_gtfs_files` e adaptada para capturar os arquivos tanto através do Google Drive quanto através do GCS por meio do novo parâmetro `upload_from_gcs` do flow `gtfs_captura_nova` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/206)
- Funções `processa_ordem_servico`, `processa_ordem_servico_trajeto_alternativo` e `processa_ordem_servico_faixa_horaria` ajustadas para considerar a coluna `tipo_os` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/206)
- Incorporadas outros nomes de colunas a serem renomeados na função `processa_ordem_servico_faixa_horaria`, bem como corrigido o tratamento de colunas ausentes (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/206)

## [1.0.8] - 2024-09-06

### Adicionado

- Cria função `processa_ordem_servico_faixa_horaria` e adiciona chamada na task `get_raw_drive_files` para processar o anexo da faixa horária (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/193)

- Adiciona materialização do modelo `ordem_servico_faixa_horaria` no flow (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/193)

## [1.0.7] - 2024-08-27

### Alterado

- Alterada a forma de identificação das tabs das planilhas de Ordem de servico e Trajetos alternativos para identificar atravez dos sufixos `ANEXO I` e `ANEXO II` respectivamente (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/171)

## [1.0.6] - 2024-08-02

### Adicionado

- Adiciona filtro para os nomes de tabs da planilha de controle os na task `get_raw_drive_files` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/128/files)

- Adiciona etapa de remover pontos antes da converção de metro para km no processamento da OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/129)

## Corrigido

## [1.0.5] - 2024-07-23

- Corrigido o parse da data_versao_gtf (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/118)

## [1.0.4] - 2024-07-17

### Adicionado

- Adiciona parametros para a captura manual do gtfs no flow `SMTR: GTFS - Captura/Tratamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/82/)

## [1.0.3] - 2024-07-04

## Corrigido

- Corrigido o formato da data salva no redis de d/m/y para y-m-d (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/91)


## [1.0.2] - 2024-06-21

### Adicionado

- Adiciona DocString nas funções de `utils.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/74)
- Adicionado log da linha selecionada para captura na task `get_os_info` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/74)

## [1.0.1] - 2024-06-20

### Corrigido

- Corrige task `get_last_capture_os` para selecionar a key salva no dicionário do Redis (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/72)

## [1.0.0] - 2024-06-19

### Adicionado

- Adicionada automação da captura do gtfs atravez da planilha Controle OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/62)


