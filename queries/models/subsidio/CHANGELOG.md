# Changelog - subsidio

## [2.0.0] - 2024-12-06

# Corrigido

- Corrigido e refatorado o modelo `viagem_transacao.sql`:
    - Reformatação conforme padrão `sqlfmt`
    - Passa a considerar registros de GPS do validador com coordenadas zeradas a partir de `DATA_SUBSIDIO_V12_INICIO`
    - Alterada janela de dados da CTE `viagem`, de forma a não ocorrer sobreposição entre viagens finalizadas na partição do dia anterior ao `start_date`
    - Passa a considerar uma transação RioCard ou Jaé para fins de validação do SBD a partir de `DATA_SUBSIDIO_V12_INICIO`

## [1.0.3] - 2024-11-29

# Alterado

- Alterada a janela de dados considerados no modelo `viagem_transacao.sql` para 6 dias (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/346)

## [1.0.2] - 2024-10-24

### Adicionado

- Adicionada exceção na verificação de viagens sem transação para a eleição de 2024-10-06 no modelo `viagem_transacao.sql` de 06h às 20h (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/286)

## [1.0.1] - 2024-09-12

### Corrigido

- Corrigido o tratamento do modelo `viagem_transacao.sql` para lidar com casos de mudança aberto/fechado ao longo da viagem, lat, long zerada do validador, mais de um validador associado ao veículo e viagem que inicia/encerra em dia diferente (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/210)

## [1.0.0] - 2024-07-31

### Adicionado

- Adicionado modelo `viagem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)