# Changelog - cadastro

## [1.3.0] - 2024-09-10

### Alterado
- Adiciona TEC no modelo `operadoras.sql`

## [1.3.0] - 2024-09-10

### Adicionado
- Cria modelo `servico_operadora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/191)

## [1.2.1] - 2024-08-02

### Alterado
- Adiciona tag `geolocalizacao` ao modelo `servicos.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)
- Adiciona tag `identificacao` ao modelo `operadoras.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)

## [1.2.0] - 2024-07-17

### Adicionado

- Cria modelos auxiliares para a tabela de servicos:
  - `aux_routes_vigencia_gtfs.sql`
  - `aux_stops_vigencia_gtfs.sql`
  - `aux_servicos_gtfs.sql`

### Alterado

- Altera estrutura do modelo `servicos.sql` para adicionar datas de inicio e fim de vigência (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98)
- Altera filtro no modelo `operadoras.sql`, deixando de filtrar operadores do modo `Fretamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98)

## [1.1.1] - 2024-04-25

### Alterado

- Adicionada coluna de modo na tabela `cadastro.consorcio`(https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)
- Adicionadas descrições das colunas modo e razão social no schema (https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)
- Adicionado source para `cadastro_staging.consorcio_modo` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)


## [1.1.0] - 2024-04-18

### Alterado

- Filtra dados dos modos Escolar, Táxi, TEC e Fretamento no modelo `operadoras.sql`
- Altera join da Jaé com o STU no modelo `operadoras.sql`, considerando o modo BRT como ônibus, para ser possível ligar a MobiRio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/273)

### Corrigido

- Reverte o tratamento do modelo `consorcios.sql` visto que a MobiRio está cadastrada na nova extração dos operadores no STU (https://github.com/prefeitura-rio/queries-rj-smtr/pull/273)

## [1.0.1] - 2024-04-16

### Corrigido

- Mudança no tratamento do modelo `consorcios.sql` para que o consórcio antigo do BRT não fique relacionado à MobiRio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/272)