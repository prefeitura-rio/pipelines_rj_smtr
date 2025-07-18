# Changelog - projeto_subsidio_sppo_encontro_contas

## [2.0.0] - 2025-07-15

### Adicionado

- Adicionada versão 2.0 dos modelos para o cálculo do Encontro de Contas do SPPO [Serviço Público de Transporte de Passageiros por Ônibus] com base no Processo.Rio MTR-PRO-2025/18086 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/511)

## [1.0.5] - 2025-05-13

### Corrigido

- Corrige referências a modelos desabilitados nos modelos `aux_balanco_rdo_servico_dia.sql`, `balanco_consorcio_ano.sql`, `balanco_consorcio_dia.sql`, `balanco_servico_ano.sql`, `balanco_servico_dia.sql`, `balanco_servico_quinzena.sql` e `receita_tarifaria_servico_nao_identificado_quinzena.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/574)

## [1.0.4] - 2024-08-29

### Alterado

- Alterado os modelos `balanco_servico_dia` e `balanco_servico_dia_pos_gt` em razão de alterações no modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

## [1.0.3] - 2024-06-07

### Adicionado

- Adiciona modelo `staging.rdo_correcao_rioonibus_servico_quinzena.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)
- Adiciona modos de execução `""` (antes das alterações do Grupo de Trabalho) e `"_pos_gt"` (após as alterações do Grupo de Trabalho) conforme Processo.Rio MTR-PRO-2024/06270 e as respectivas alterações dos nomes das tabelas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)
- Adiciona novo tratamento de serviços do RDO no modelo `balanco_servico_dia_pos_gt.sql` com base no modelo `balanco_servico_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)
- Adiciona e altera descrições no schema.yml dos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/332)

## [1.0.2] - 2024-05-21

### Alterado

- Adiciona refs e sources de modelos do dbt (https://github.com/prefeitura-rio/queries-rj-smtr/pull/319)

## [1.0.1] - 2024-05-16

### Alterado

- Refatora nome de colunas e adiciona schema.yml (https://github.com/prefeitura-rio/queries-rj-smtr/pull/306)

## [1.0.0] - 2024-05-14

### Adicionado

- Adiciona tabela de cálculo do encontro de contas por dia e serviço + agregações (https://github.com/prefeitura-rio/queries-rj-smtr/pull/304)

### Removido
- Versões de teste: https://github.com/prefeitura-rio/queries-rj-smtr/pull/234, https://github.com/prefeitura-rio/queries-rj-smtr/pull/233
