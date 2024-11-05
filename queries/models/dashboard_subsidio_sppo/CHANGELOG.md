# Changelog - dashboard_subsidio_sppo

## [7.0.6] - 2024-11-05

### Alterado

- Altera regras do modelo `viagens_remuneradas` conforme Resolução SMTR N° 3777/2024 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/300)

## [7.0.5] - 2024-10-16

### Corrigido

- Corrigido join em `sumario_servico_dia_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/271)

## [7.0.4] - 2024-09-27

### Corrigido

- Remove agrupamento de viagens planejadas em `sumario_servico_dia` e `sumario_servico_dia_tipo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/250)

## [7.0.3] - 2024-09-20

### Alterado

- Remove kms de veículos "Não licenciado" e "Não vistoriado" do cálculo do pof na CTE `servico_faixa_km_apuracao` do modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/239)

## [7.0.2] - 2024-08-29

### Alterado

- Alterado os modelos `sumario_dia`, `sumario_servico_dia` e `sumario_servico_dia_tipo` em razão de alterações no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

- Alterado modelo `viagens_remuneradas` em razão da apuração por faixa horária (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

## [7.0.1] - 2024-08-19

## Adicionado

- Adicionada coluna `datetime_ultima_atualizacao` na tabela `sumario_servico_dia_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/147)

## [7.0.0] - 2024-07-31

### Alterado

- Refatora tabelas `viagens_remuneradas.sql` e
  `sumario_servico_dia_tipo.sql` para uso da nova tabela de
  referência (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)
- Adiciona novo tipo de viagem `"Sem transação"` nas tabelas
  `viagens_remuneradas.sql`,
  `sumario_servico_dia_tipo.sql`,
  `sumario_servico_dia_tipo_sem_glosa.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)

## [6.0.0] - 2024-04-18

### Alterado

- Adiciona novo filtro no modelo `sumario_servico_dia_historico.sql` em razão de alterações no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

## [5.0.0] - 2024-03-30

### Adicionado

- Nova tabela de referência do valor do subsídio por tipo de viagem,
  inclusão do novo tipo `"Não vistoriado"`:
  `subsidio_valor_km_tipo_viagem.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)

### Alterado

- Refatora tabelas `viagens_remuneradas.sql` e
  `sumario_servico_dia_tipo_sem_glosa.sql` para uso da nova tabela de
  referência (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Adiciona novo tipo de viagem/status `"Não vistoriado"` nas tabelas
  `sumario_servico_dia_tipo.sql`,
  `sumario_servico_dia_tipo_sem_glosa.sql`,
  `sumario_servico_tipo_viagem_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Simplifica padronização dos tipos de viagens nas tabelas
  `sumario_servico_dia_tipo.sql` e `sumario_servico_tipo_viagem_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Atualiza documentação de tabelas e colunas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Alterações feitas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/229 e https://github.com/prefeitura-rio/queries-rj-smtr/pull/236 corrigidas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/239
