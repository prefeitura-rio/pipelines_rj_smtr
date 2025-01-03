# Changelog - monitoramento

## [1.2.3] - 2025-01-03

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