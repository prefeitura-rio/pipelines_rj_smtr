# Changelog - br_rj_riodejaneiro_rdo

## [1.0.0] - 2024-10-02

### Alterado
- Muda lógica de materialização, selecionando o dataset `br_rj_riodejaneiro_rdo` inteiro (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/254)
- Adiciona variavel `version` na materialização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/254)
- Adiciona captura do RDO SPPO, RDO STPL e RHO STPL no flow de tratamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/254)
- Renomeia os flows (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/254):
    - de `SMTR: SPPO RHO - Materialização (subflow)` para `SMTR: RDO - Materialização (subflow)`
    - de `SMTR: RDO - Captura` para `SMTR: RDO - Captura (subflow)`

### Corrigido
- Adiciona tratamento de encoding na leitura dos arquivos capturados (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/254)
