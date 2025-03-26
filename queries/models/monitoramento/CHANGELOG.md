# Changelog - monitoramento

## [1.3.5] - 2025-03-25

### Alterado

- Alterado a lógica dos modelos `gps_segmento_viagem` e `viagem_validacao` para o monitoramento de viagens (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/458)

## [1.3.4] - 2025-02-27

### Corrigido
- Corrige os valores as colunas `valor_subsidio_pago` e `valor_penalidade` antes da apuração por faixa no modelo `monitoramento_servico_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/465)

## [1.3.2] - 2025-02-21

### Alterado
- Torna filtro de partição obrigatório no modelo `gps_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/448)

## [1.3.1] - 2025-02-03

### Corrigido

- Transforma dados em branco em nulos no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/408)


## [1.3.0] - 2025-01-22

### Alterado

- Adiciona as colunas `indicador_servico_planejado_gtfs`, `indicador_servico_planejado_os`, `indicador_servico_divergente` e `indicador_shape_invalido` no modelo `viagem_validacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

- Adiciona a coluna `indicador_servico_divergente` no modelo `gps_segmento_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

- Remove tratamento de sentido no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

### Corrigido

- Corrige trip_id inteiro nos modelos `staging_viagem_informada_rioonibus.sql` e `staging_viagem_informada_brt.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

- Corrige filtro incremental no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

## [1.2.2] - 2025-01-08

### Adicionado

- Cria modelos `sumario_servico_dia_pagamento_historico.sql` e `sumario_servico_dia_tipo_viagem_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/258)

- Adicionado o label `dashboard` aos modelos `sumario_servico_dia_pagamento_historico.sql` e `sumario_servico_dia_tipo_viagem_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/258)

## [1.2.1] - 2025-01-03

### Adicionado
- Cria modelo `monitoramento_viagem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/372)

## [1.2.0] - 2024-11-28

### Adicionado
- Cria modelo `staging_viagem_informada_brt.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)

### Alterado
- Adiciona viagens BRT no modelo: `gps_viagem.sql` e `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)
- Altera data hardcoded por variável no modelo `gps_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)
- Cria corte de viagens na execução full nos modelos `gps_viagem.sql` e `gps_segmento_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)

## [1.1.0] - 2024-11-08

### Adicionado
- Cria modelos de validação de viagens: `gps_viagem.sql`, `gps_segmento_viagem.sql` e `viagem_validacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

### Alterado
- Adiciona coluna `modo` no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

## [1.0.1] - 2024-10-23

### Corrigido
- Remove fuso horário na conversão para data do campo data_viagem no modelo `staging_viagem_informada_rioonibus.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/284)

## [1.0.0] - 2024-10-21

### Adicionado
- Cria modelos para tratamento de viagens informadas: `staging_viagem_informada_rioonibus.sql` e `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/276)
