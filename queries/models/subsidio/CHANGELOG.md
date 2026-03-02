# Changelog - subsidio

## [2.0.6] - 2026-03-02

### Alterado

- Altera o modelo `percentual_operacao_faixa_horaria.sql` para desconsiderar a `distancia_planejada` no cálculo de `pof` de viagens do tipo `Não licenciado` e `Não vistoriado` a partir de `2024-09-01` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/510)

## [2.3.2] - 2026-01-15

### Adicionado

- Adiciona o modelo em staging `staging_valor_tipo_penalidade` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1171)

### Alterado

- Move o modelo `valor_tipo_penalidade` do `dashboard_subsidio_sppo` para o `subsidio` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1171)
- Altera o modelo `viagem_transacao_aux_v2` para incluir viagens do dia anterior fora do ambiente de produção (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1108)

## [2.3.1] - 2026-01-06

### Alterado

- Altera o modelo `viagem_classificada` para exclusão de registros de autuações com `status` igual a "cancelado" (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1147)

## [2.3.0] - 2025-12-26

### Alterado

- Altera modelo `viagem_regularidade_temperatura` para reprocessamento dos descontos por inoperabilidade da climatização em OUT/Q2 e NOV/Q1 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1134)

## [2.2.9] - 2025-12-09

### Alterado

- Altera modelo `viagem_regularidade_temperatura` para retomada dos descontos por inoperabilidade da climatização, conforme Evento 112 do PROCEDIMENTO COMUM CÍVEL Nº 3019687-30.2025.8.19.0001/RJ (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1096)

## [2.2.8] - 2025-12-08

### Alterado

- Altera modelo `staging_servico_contrato_abreviado` de acordo com alterações na planilha de serviços com contrato abreviado (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1089)

## [2.2.7] - 2025-11-27

### Alterado

- Altera variável do modelo `aux_viagem_temperatura`  de `DATA_SUBSIDIO_V21_INICIO` para `DATA_SUBSIDIO_V99_INICIO`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1068)

## [2.2.6] - 2025-11-26

### Alterado

- Altera data do modelo `viagem_regularidade_temperatura` para interrupção das glosas por climatização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1069)

## [2.2.5] - 2025-11-14

### Alterado

- Altera referência dos modelos `servico_contrato_abreviado` e `valor_km_tipo_viagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1058)

## [2.2.4] - 2025-10-28

### Alterado

- Ajusta o cálculo do `indicador_regularidade_ar_condicionado_viagem`no modelo `viagem_regularidade_temperatura`, incluindo a condição em que `indicador_temperatura_nula_viagem` é igual a True. (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/993)

- Revertida a alteração da data de início das glosas `Validador fechado` e `Validador associado incorretamente` no modelo `viagem_transacao_aux_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1002)

## [2.2.3] - 2025-10-27

### Alterado

- Alterada a data de início das glosas `Validador fechado` e `Validador associado incorretamente` para `2025-10-01` no modelo `viagem_transacao_aux_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/995)

- Criada exceção para os tipo_viagem `Validador associado incorretamente` no dia `2025-10-10` no modelo `viagem_transacao_aux_v2` conforme o email `2025-10-10T15:08` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/995)

- Altera lógica do modelo `aux_viagem_temperatura` para filtrar pontos de GPS fora das garagens e endereços de manutenção dos validadores no cálculo do `indicador_gps_servico_divergente` e `indicador_estado_equipamento_aberto` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/996)

## [2.2.2] - 2025-10-24

### Adicionado

- Adiciona o teste `test_check_tecnologia_minima` no modelo `viagem_classificada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/990)

### Corrigido

- Corrige variável `DATA_SUBSIDIO_V17_INICIO` para `DATA_SUBSIDIO_V16_INICIO` no modelo `viagem_classificada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/990)

## [2.2.1] - 2025-10-23

### Corrigido

- Corrige o `indicador_estado_equipamento_aberto` quando o `id_validador` é nulo nos modelos `viagem_transacao_aux_v1` e `aux_viagem_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/985)

## [2.2.0] - 2025-10-22

### Alterado

- Altera nome da CTE `temperatura_inmet` para `temperatura_inmet_alertario` no modelo `aux_viagem_temperatura` e troca referência para utilizar o modelo `temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/977)

## [2.1.9] - 2025-10-06

### Adicionado

- Adicionado o modelo de dicionário `dicionario_subsidio` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/926)

## [2.1.8] - 2025-09-29

### Corrigido

- Corrige a data de início da verificação do `indicador_falha_recorrente` no modelo `viagem_regularidade_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/904)

- Corrige o teste `test_check_regularidade_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/904)

## [2.1.7] - 2025-09-26

### Alterado

- Altera critérios para seleção do id_validador no modelo `aux_viagem_temperatura`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/902)

### Corrigido

- Corrige o cálculo de percentual de temperatura regular no modelo `aux_viagem_temperatura`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/902)

## [2.1.6] - 2025-09-17

### Alterado

- Altera nome do indicador `indicador_temperatura_nula_zero_viagem` para criar `indicador_temperatura_zero_viagem` e `indicador_temperatura_nula_viagem`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/874)
- Altera lógica das CTEs para o nível de agregação do validador por viagem (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/874)
- Altera colunas percentuais adicionando uma multiplicação por 100 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/874)
- Altera coluna `quantidade_nula_zero` separando em `quantidade_nula` e `quantidade_zero` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/874)

### Corrigido

- Corrige CTE `classificacao_temperatura` adicionando maior ou igual na diferença da temperatura externa pela interna (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/874)

## [2.1.5] - 2025-09-15

### Corrigido

- Corrigida a data de inicio da tecnologia_remunerada no modelo `viagem_classificada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/790)
- Corrigida a coluna tecnologia_remunerada no modelo `viagem_transacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/790)
- Corrigida o modelo `viagem_transacao_aux_v1` para datas anteriores a `2025-04-01` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/790)

## [2.1.4] - 2025-09-02

### Alterado

- Altera lógica da cte `particoes_completas` no modelo `aux_viagem_temperatura` adicionando um inner join para materializar os dados atuais somente se ainda existirem nos dados novos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/835)
- Alterado lógica dos modelos `viagem_transacao_aux_v1` e `viagem_transacao_aux_v2` trocando a `DATA_SUBSIDIO_V18_INICIO` pela `DATA_SUBSIDIO_V99_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/829)

## [2.1.3] - 2025-08-19

### Adicionado

- Adiciona lógica das colunas de controle no modelo `aux_viagem_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/796)

### Alterado

- Altera lógica para classificar viagens como `Sem transação`, `Validador fechado` e `Validador associado incorretamente` após `DATA_SUBSIDIO_V99_INICIO` no modelo `viagem_transacao_aux_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/796)

## [2.1.2] - 2025-08-15

### Alterado

- Altera lógica para considerar `indicador_falha_recorrente` após `DATA_SUBSIDIO_V99_INICIO` no modelo `viagem_regularidade_temperatura`, alteração referente ao Processo.rio `MTR-CAP-2025/25179` e `MTR-MEM-2025/02246` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/788)

## [2.1.1] - 2025-08-14

### Alterado

- Refatora modelos `aux_viagem_temperatura` e `viagem_regularidade_temperatura`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/787)

## [2.1.0] - 2025-08-11

### Corrigido

- Corrigida a coluna `tecnologia_remunerada` nos modelos `viagem_transacao` e `viagem_transacao_aux_v1` assim como a data limite do modelo `viagem_transacao_aux_v1` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/782)

## [2.0.9] - 2025-08-08

### Alterado

- Altera a data para regra de `Validador associado incorretamente` nos modelos `viagem_transacao_aux_v2` `viagem_transacao_aux_v1` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/769)

## [2.0.8] - 2025-08-07

### Alterado

- Alterado o modelo `percentual_operacao_faixa_horaria` para apuração por sentido e utilização do versionamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

## [2.0.7] - 2025-07-31

### Adicionado

- Cria modelos `aux_viagem_temperatura`, `viagem_transacao_aux_v1`, `viagem_transacao_aux_v2` e `viagem_regularidade_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Adiciona testes para os modelos `aux_viagem_temperatura` e `viagem_regularidade_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Adiciona as colunas `placa` e `ano_fabricacao` no modelo `viagem_classificada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

### Alterado

- Altera o modelo `viagem_transacao` para utilizar os modelos `viagem_transacao_aux_v1` e `viagem_transacao_aux_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Altera testes do modelo `viagem_classificada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

## [2.0.6] - 2025-07-03

### Adicionado

- Cria modelo `viagem_classificada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)
- Adiciona as colunas `modo`, `tecnologia_apurada`, `tecnologia_remunerada` e `sentido` no modelo `viagem_transacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

### Alterado

- Refatora a coluna `id_validador` para incluir a lista de validadores que classificaram a viagem no modelo `viagem_transacao_aux` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

## [2.0.5] - 2025-06-27

### Corrigido

- Corrigido a coluna `id_validador` e o agrupamento do estado do equipamento no modelo `viagem_transacao_aux.sql`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)

## [2.0.4] - 2025-06-25

### Alterado

- Altera fonte dos dados de veículo para `aux_veiculo_dia_consolidada` no modelo `viagem_transacao_aux.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [2.0.3] - 2025-06-24

# Adicionado

- Cria modelos `percentual_operacao_faixa_horaria` e `servico_contrato_abreviado`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)
- Adiciona lógica para novos tipos de viagem conforme termos da [RESOLUÇÃO SMTR Nº 3843/2025](https://doweb.rio.rj.gov.br/portal/visualizacoes/pdf/7371/#/p:14/e:7371) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)

## [2.0.2] - 2025-01-21

# Adicionado

- Adiciona o modelo `valor_km_tipo_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)
- Adiciona a coluna `tecnologia` nos modelos  `viagem_transacao.sql` e `viagem_transacao_aux.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [2.0.1] - 2025-01-06

# Corrigido

- Corrigido e refatorado o modelo `viagem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/372)

# Adicionado

- Adicionado o modelo `viagem_transacao_aux.sql`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/372)

## [2.0.0] - 2024-12-06

# Corrigido

- Corrigido e refatorado o modelo `viagem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/357):
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
