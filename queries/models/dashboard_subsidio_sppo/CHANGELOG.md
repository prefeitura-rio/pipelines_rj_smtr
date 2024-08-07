# Changelog - dashboard_subsidio_sppo

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
