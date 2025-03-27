# Changelog - planejamento

## [1.3.5] - 2025-03-27

### Corrigido

- Corrigido o tipo_dia de `2025-03-02` no modelo `aux_calendario_manual.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/)

## [1.3.4] - 2025-03-25

### Alterado

- Altera o tipo_os e tipo_dia entre `2025-03-01` e `2025-03-05` no modelo `aux_calendario_manual.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/495)
- Altera modelo `tecnologia_servico` em razão da inclusão de vigência do tipo de tecnologia (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/493)

### Adicionado

- Cria modelos `staging_tecnologia_servico` e `tecnologia_prioridade` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/493)
- Cria modelos: `aux_os_sppo_faixa_horaria_sentido_dia`, `aux_ordem_servico_faixa_horaria` [Temporário] e `servico_planejado_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/458)

## [1.3.3] - 2025-03-14

### Adicionado

- Adiciona novas datas no modelo `aux_calendario_manual.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/480)
## [1.3.2] - 2025-03-10

### Alterado

- Ativa materialização do modelo `viagem_planejada_planejamento.sql` a partir de `2025-01-02` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/471)

## [1.3.1] - 2025-01-27

### Adicionado

- Cria modelo `tecnologia_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [1.3.0] - 2024-12-30

### Adicionado

- Cria modelo `matriz_integracao.sql` com a matriz publicada pela SMTR (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)

## [1.2.0] - 2024-12-04

### Adicionado

- Cria modelos para tabela de viagens planejadas de acordo com o GTFS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/352):
  - `aux_frequencies_horario_tratado.sql`
  - `aux_ordem_servico_horario_tratado.sql`
  - `aux_trips_dia.sql`
  - `viagem_planejada_planejamento.sql`

### Alterado
- Adiciona colunas start_pt e end_pt no modelo `shapes_geom_planejamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/352)

## [1.1.0] - 2024-11-08

### Adicionado

- Cria modelos para tabela de calendario: `aux_calendario_manual.sql` e `calendario.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)
- Cria modelos para divisão de shapes: `aux_segmento_shape.py`, `aux_shapes_geom_filtrada.sql`, `shapes_geom_planejamento.sql` e `segmento_shape.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

## [1.0.2] - 2024-11-07

### Alterado

- Refatora modelo `ordem_servico_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/306)

## [1.0.1] - 2024-10-30

### Alterado

- Alterado modelo `ordem_servico_faixa_horaria` em razão das novas faixas horárias (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/295)

## [1.0.0] - 2024-08-29

### Adicionado

- Cria modelo `ordem_servico_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)