# Changelog - subsidio

## [1.0.2] - 2024-10-24

- cria exceção na verificação d viagens sem transacao para a eleição de 2024-10-06 em viagem_transacao de 06h as 20h (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/286)

## [1.0.1] - 2024-09-12

### Corrigido

- Corriido o tratamento de `viagem_transacao` para lidar com casos de mudança aberto/fechado ao longo da viagem, lat, long zerada do validador, mais de um validador associado ao veículo e viagem que inicia/encerra em dia diferente(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/210)

## [1.0.0] - 2024-07-31

### Adicionado

- Cria modelo `viagem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)