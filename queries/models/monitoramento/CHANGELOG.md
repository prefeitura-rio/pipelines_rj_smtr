# Changelog - monitoramento

## [1.5.3] - 2025-06-30

### cORRIGIDO

- Corrigida a verificação do status `Registrado com ar inoperante` no modelo `veiculo_dia.sql`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/652)

## [1.5.2] - 2025-06-27

### Adicionado

- Adicionado limite de data para utilização dos dados do modelo `sppo_veiculo_dia.sql` no modelo`aux_veiculo_dia_consolidada.sql`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)
- Adicionada exceção para as datas de autuações disciplinares no modelo `veiculo_dia.sql` para considerar tambem autuações com `data_inclusao_datalake` = `2025-06-25`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

## [1.5.1] - 2025-06-26

### Removido

- Remove a coluna `modo` do modelo `aux_veiculo_dia_consolidada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)


## [1.5.0] - 2025-06-25

### Adicionado

- Cria modelos `autuacao_disciplinar_historico.sql`, `aux_veiculo_dia_consolidada.sql`, `aux_veiculo_gps_dia.sql` e `veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)
- Adiciona coluna `id_execucao_dbt` no modelo `veiculo_fiscalizacao_lacre.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

### Alterado

- Move modelo `infracao_staging.sql` do dataset `veiculo_staging` para `monitoramento_staging` e renomeia para `staging_infracao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [1.4.2] - 2025-06-24

### Alterado

- Alterado o cálculo da `km_apurada` no modelo `monitoramento_servico_dia_v2.sql` para somar a quilometragem dos veículos não licenciados e não vistoriados (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/629)
- Altera lógica do modelo  `gps_segmento_viagem` para considerar vigência da camada dos túneis (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/617)

## [1.4.1] - 2025-06-06

### Adicionado

- Adiciona testes nos modelos `staging_gps`, `staging_realocacao` e `gps` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/614)

## [1.4.0] - 2025-05-29

### Alterado

- Altera lógica de validação de viagens no modelo `viagem_validacao` [quantidade_segmentos_validos >= quantidade_segmentos_necessarios] (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/585)
- Altera modelo `gps_viagem` adicionando dados de GPS da API da Cittati (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/585)

## [1.3.9] - 2025-05-28

### Adicionado

- Cria modelos `staging_veiculo_fiscalizacao_lacre.sql` e `veiculo_fiscalizacao_lacre.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/594)

## [1.3.8] - 2025-05-28


### Adicionado

 - Cria modelo `gps_15_minutos_union` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/586)

### Alterado

- Altera lógica do modelo `aux_gps_trajeto_correto` para considerar trajetos alternativos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/595)

## [1.3.7] - 2025-05-20

### Adicionado

 - Cria modelos `gps`, `gps_15_minutos`, `aux_gps`, `aux_gps_filtrada`, `aux_gps_parada`, `aux_gps_realocacao`, `aux_gps_trajeto_correto`, `aux_gps_velocidade` ,`aux_realocacao` e `staging_garagens`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/297)

## [1.3.6] - 2025-05-13

### Corrigido

- Corrige referências a modelos desabilitados nos modelos `monitoramento_servico_dia.sql`, `monitoramento_servico_dia_tipo_viagem.sql`, `monitoramento_servico_dia_historico.sql` e `monitoramento_servico_dia_tipo_viagem_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/574)

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
