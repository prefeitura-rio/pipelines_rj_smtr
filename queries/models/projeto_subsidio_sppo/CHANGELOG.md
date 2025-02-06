# Changelog - projeto_subsidio_sppo

## [9.1.7] - 2025-02-05

### Adicionado

- Altera os dias `2025-01-18`, `2025-01-19`, `2025-01-20`,  `2025-01-25` e `2025-01-26` para tipo_dia `Verão` no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/416)

## [9.1.6] - 2025-01-23

### Adicionado

- Altera os dias `2025-01-11` e `2025-01-12` para tipo_dia `Verão` no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/398)

## [9.1.5] - 2025-01-08

### Adicionado

- Adicionado label `dashboard` ao modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/258)

## [9.1.4] - 2025-01-06

### Adicionado

- Adicionadas datas com os tipo os `Fim de ano`, `Reveillon` e tipo dia `Ponto facultativo` no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/383)

## [9.1.3] - 2025-01-03

### Alterado

- Alterado a data final no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/378)

## [9.1.2] - 2024-12-18

### Alterado

- Alterado o tipo os de `2024-12-07`, `2024-12-08`, `2024-12-14` e `2024-12-15` para `Extraordinária - Verão` no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/365)

## [9.1.1] - 2024-12-13

### Adicionado

- Adiciona coluna `velocidade_media` e `datetime_ultima_atualizacao` no modelo `viagem_conformidade.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/340)
- Adiciona coluna `velocidade_media` no modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/340)
- Adiciona filtro para remover as viagens com velocidade média superior a 110 km/h, exceto os serviços com itinerários complexos - não circulares com distância linear entre início e fim inferior a 2 km e mais de uma interseção entre o buffer de início/fim e o itinerário - no modelo `viagem_completa.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/340)

## [9.1.0] - 2024-12-02

### Corrigido

- Corrigido o cálculo de quilometragem e viagens nas faixas horárias de 24h às 27h e 00h às 03h na mudança de feed do GTFS no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/349)

## [9.0.9] - 2024-11-29

### Corrigido

- Corrigida a origem da coluna `distancia_total_planejada` na faixa horária de 24h às 27h no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/346)

## [9.0.8] - 2024-11-28

### Alterado

- Alterado o tipo_os de `2024-11-24` para `Parada LGBTQI+` no modelo `subsidio_data_versao_efetiva` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/345)

## [9.0.7] - 2024-11-11

### Alterado

- Alterado o tipo_os de `2024-11-03` e `2024-11-10` e  para `Enem`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/311)

- Alterado o tipo_dia de `2024-10-21`, `2024-10-28`, `2024-11-18`, `2024-11-19` e  para `Ponto Facultativo`  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/311)

## [9.0.6] - 2024-10-22

### Alterado

- Alterado o tipo_os de 2024-10-06 para `Eleição` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/281)

## [9.0.5] - 2024-10-08

### Corrigido

- Corrigido a duplicação de viagens no modelo `viagens_planejadas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/266)

## [9.0.4] - 2024-10-07

### Alterado

- Alterados os tipo_os de `13/09/24` a `15/09/24` e `19/09/24` a `22/09/24` para `Rock in Rio` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/264)

## [9.0.3] - 2024-09-23

### Alterado

- Adicionado subtipo_dia `Verão` em `2024-09-14` e `2024-09-15` no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/242)

## [9.0.2] - 2024-09-17

### Corrigido

- corrigidas as referencias a tabela `gps_sppo` em `aux_registro_status_trajeto` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/234)

### Alterado

- Alterada a consulta por shapes em `aux_registro_status_trajeto` para buscar o feed_start_date do particionamento da tabela shapes_geom em `data_versao_efetiva`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/234)

## [9.0.1] - 2024-09-11

### Alterado

- Adicionado subtipo_dia `CNU` em `2024-08-18` conforme processo.rio MTR-PRO-2024/13252 no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/208)

## [9.0.0] - 2024-08-29

### Alterado

- Alterado os modelos `viagem_planejada` e `aux_registros_status_trajeto` em razão da apuração por faixa horária (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

- Alterado modelo `subsidio_data_versao_efetiva` para materializar apenas 1 dia (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

## [6.0.4] 2024-08-13

### Adicionado

- Adicionado filtro para remover viagens do serviço SE001 da apuração do subsídio (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/144)

## [6.0.3] - 2024-08-01

### Alterado

- Alterados modelos `viagem_planejada.sql` e `subsidio_data_versao_efetiva.sql` para materializar sempre em D+0 e permitir acompanhamento pelos operadores (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/125)

## [6.0.2] - 2024-04-22

### Adicionado

- Adicionado planejamento de Abril/Q2/2024 no modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/279)
- Adicionado `2024-04-22` como `Ponto Facultativo` em razão do [`DECRETO RIO Nº 54267/2024`](https://doweb.rio.rj.gov.br/apifront/portal/edicoes/imprimir_materia/1046645/6539) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/279)

### Corrigido

- Corrigido e refatorado o tratamento do modelo `subsidio_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/279)

## [6.0.1] - 2024-04-19

### Corrigido

- Corrige união do modelo `viagem_planejada.sql` com o modelo o `ordem_servico_trips_shapes_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/278)

## [6.0.0] - 2024-04-18

### Adicionado

- Adicionada descrição dos modelos `subsidio_shapes_geom.sql`, `subsidio_trips_desaninhada.sql` e `subsidio_quadro_horario.sql`, bem como
informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Adicionada descrição do modelo `ssubsidio_data_versao_efetiva.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Movidos os modelos `subsidio_shapes_geom.sql`, `subsidio_trips_desaninhada.sql` e `subsidio_quadro_horario.sql` para a pasta `deprecated` em razão de terem sido descontinuados (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Incluídas as colunas `subtipo_dia`, `feed_version`, `feed_start_date` e `tipo_os` no modelo `subsidio_data_versao_efetiva.sql`. O modelo passa a possuir queries diferentes, caso a `run_date` seja antes ou depois do `SUBSIDIO_V6` (`2024-04-01`). Essas colunas permanecerão nulas, caso a tabela seja executada antes dessa data (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Alterada para `ORDER BY perc_conformidade_shape DESC, id_tipo_trajeto` para seleção de viagem entre múltiplos trajetos a partir `SUBSIDIO_V6` (`2024-04-01`) no modelo `viagem_completa.sql` de forma a privilegiar, em caso do mesmo `perc_conformidade_shape`, o trajeto regular em detrimento do alternativo (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Incluídas as colunas `id_tipo_trajeto` e `feed_version` no modelo `viagem_planejada.sql` Essas colunas permanecerão nulas, caso a tabela seja executada antes dessa data. O modelo passa a ter duas queries, caso a `run_date` seja antes ou depois do `SUBSIDIO_V6` (`2024-04-01`). A partir dessa data, o modelo passa a depender exclusivamente das tabelas de `gtfs`, descontinuando os modelos `subsidio_shapes_geom.sql`, `subsidio_trips_desaninhada.sql` e `subsidio_quadro_horario.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Alterada descrição do modelo `viagem_planejada.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Corrigido

- A partir da `SUBSIDIO_V6` (`2024-04-01`), os dados de GPS no modelo `aux_registros_status_trajeto.sql` são sempre comparados com os dados de planejamento da data de operação, bem como também serão particionados na data de operação. Com isso, viagens que iniciam em um dia e encerram no outro (`overnight`) passam a ser identificadas e seus registros sempre armazenados na data de operação, independentemente de alteração de planejamento entre as datas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- A partir da `SUBSIDIO_V6` (`2024-04-01`), as potenciais viagens identificadas no modelo `aux_viagem_inicio_fim.sql` serão filtradas apenas para as viagens iniciadas na data de operação, de forma a não duplicar viagens em partições diferentes (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- A partir da `SUBSIDIO_V6` (`2024-04-01`), são considerados no modelo `aux_viagem_registros.sql` apenas os registros na data de operação
