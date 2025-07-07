# Changelog - financeiro_staging

## [1.0.3] - 2025-07-01

### Alterado

- Altera o modelo `subsidio_faixa_servico_dia.sql` para desconsiderar a `distancia_planejada` no cálculo de `pof` de viagens do tipo `Não licenciado` e `Não vistoriado` a partir de `2024-09-01` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/510)

## [1.0.2] - 2025-06-24

### Removido

- Remove o modelo `subsidio_faixa_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)

## [1.0.1] - 2024-09-20

### Alterado

- Remove kms de veículos "Não licenciado" e "Não vistoriado" do cálculo do pof no modelo `subsidio_faixa_servico_dia`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/239)

## [1.0.0] - 2024-09-02

### Adicionado

- Cria modelo `subsidio_faixa_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)