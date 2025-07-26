/*
Documentação do Modelo
======================================================

1. Descrição Geral do Modelo
-----------------------------
Este modelo é o núcleo do processo de construção da Matriz Origem-Destino (OD).
Ele transforma registros brutos e individuais de transações de bilhetagem em uma
matriz final que mostra a média de viagens diárias entre zonas geográficas (hexágonos H3).
A análise é baseada em uma amostra de passageiros identificados (que possuem cartões e
realizaram mais de uma transação no período) e serve como uma "matriz semente" para
etapas futuras de expansão e calibração.

2. Lógica de Negócio Essencial (Passo a Passo)
-----------------------------------------------
O modelo executa uma sequência complexa de transformações:
- **Pré-filtragem:** Clientes com apenas uma transação no período são removidos, pois não é
  possível inferir um deslocamento.
- **Construção de Viagens (Recursão):** O passo mais complexo. Transações sequenciais de um
  mesmo cliente são agrupadas em "viagens" utilizando uma CTE Recursiva, com base em duas
  regras de tempo:
    - **Janela de 4 horas:** Transações dentro de 4 horas do início da viagem são
      consideradas parte da mesma.
    - **Gap de 24 horas:** A sequência de atividade de um cliente é interrompida se houver
      um intervalo maior que 24 horas entre duas transações consecutivas.
- **Lógica de Destino Híbrida:** O destino de uma viagem é definido como a localização da
  próxima atividade do cliente (destino preditivo). Caso seja a última atividade do
  cliente, o destino é a localização da última transação da própria viagem.
- **Filtragem de Viagens Válidas:** Viagens de uma única transação são descartadas, a menos
  que sejam seguidas por outra viagem do mesmo cliente. Isso garante que os resultados
  representem deslocamentos reais.
- **Agregação Final:** As viagens válidas são agrupadas por Origem, Destino e Tipo de Dia
  para calcular a média de viagens diárias para cada rota.

3. Particularidades e Premissas
--------------------------------
- **Tratamento de Viagens da Madrugada:** O modelo busca transações de um dia além do
  período solicitado para capturar corretamente as viagens que começam no final do último
  dia e terminam na madrugada do dia seguinte. O filtro final garante que apenas as viagens
  *iniciadas* no período de interesse sejam contabilizadas.
- **Amostra vs. Universo:** A matriz gerada representa os padrões da amostra de passageiros
  identificados. A premissa é que este padrão pode ser usado para estimar o comportamento
  do universo total de passageiros em modelos subsequentes.

4. Entradas e Saídas Principais
--------------------------------
- **Entradas:** `calendario`, `h3_res8`, `transacao`.
- **Saída:** Tabela agregada com as colunas: `tipo_dia`, `subtipo_dia`, `origem_id`,
  `destino_id`, `total_viagens`, `total_dias`, `media_viagens_dia`, e metadados de execução.

*/

{{
    config(
        materialized="table",
    )
}}

{#
Jinja Set Block: incremental_filter
- Propósito: Define um bloco de código SQL reutilizável para a filtragem de datas.
- Lógica: Cria uma cláusula `BETWEEN` que utiliza variáveis do dbt (`date_range_start`, `date_range_end`).
           Isso permite que o intervalo de datas da consulta seja alterado dinamicamente
           sem modificar o código principal, por exemplo, via linha de comando do dbt.
#}
{% set incremental_filter %}
    data
    between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
{% endset %}

with recursive
    /*
Etapa 1: calendario
- Propósito: Carregar informações sobre os dias do período de análise.
- Entrada: Modelo dbt `calendario` (via `ref`).
- Lógica: Filtra o calendário usando a variável Jinja `incremental_filter`.
- Saída: Tabela de referência com `data`, `tipo_dia` e `subtipo_dia`.
*/
    calendario as (
        select data, tipo_dia, subtipo_dia
        from {{ ref("calendario") }}
        where {{ incremental_filter }}
    ),
    /*
Etapa 2: hex
- Propósito: Carregar a grade geográfica de hexágonos (H3).
- Entrada: Fonte de dados `br_rj_riodejaneiro_geo.h3_res8` (via `source`).
- Lógica: Converte a geometria de texto para o tipo GEOGRAPHY.
- Saída: Tabela de lookup que mapeia cada `tile_id` à sua forma geométrica.
*/
    hex as (select tile_id, geometry_hex from {{ ref("h3_res8") }}),
    /*
Etapa 3: transacao
- Propósito: Coletar as transações relevantes e associá-las a uma localização.
- Entrada: Modelo dbt `transacao` e a CTE `hex`.
- Lógica: Filtra transações por data (usando `incremental_filter`) e cliente, e realiza
           um JOIN Espacial (`ST_CONTAINS`) para encontrar o `tile_id` de cada transação.
- Saída: Lista de transações "limpas" com `hash_cliente` e `tile_id`.
*/
    transacao as (
        select t.data, t.datetime_transacao, h.tile_id, t.hash_cliente
        from {{ ref("transacao") }} as t
        join hex as h on st_contains(h.geometry_hex, t.geo_point_transacao)
        where
            between date("{{ var('date_range_start') }}") and date_add(date("{{ var('date_range_end') }}"),interval 1 day)
            and (
                to_base64(hash_cliente)
                != "NnRh5t0HvbVzQstksqjY4PoTxThCqV7CCpDTW91ut38="
            )  -- Exclui cartões avulsos
            and hash_cliente is not null  -- Exclui transações sem cliente
    ),
    /*
Etapa 4: transacao_contada
- Propósito: Contar o total de transações para cada cliente no período.
- Entrada: CTE `transacao`.
- Lógica: Usa a função de janela `COUNT(*) OVER (PARTITION BY hash_cliente)`.
- Saída: Transações anotadas com o total de atividade de seu respectivo cliente.
*/
    transacao_contada as (
        select
            data,
            hash_cliente,
            datetime_transacao,
            tile_id,
            count(*) over (partition by hash_cliente) as total_transacoes_cliente
        from transacao
    ),
    /*
Etapa 5: transacao_filtrada
- Propósito: Remover clientes com atividade insuficiente para análise de deslocamento.
- Entrada: CTE `transacao_contada`.
- Lógica: Mantém apenas transações de clientes com mais de 1 transação no total.
- Saída: Transações apenas de clientes "ativos", elegíveis para análise de viagem.
*/
    transacao_filtrada as (
        select data, hash_cliente, datetime_transacao, tile_id
        from transacao_contada
        where total_transacoes_cliente > 1
    ),
    /*
Etapa 6: transacao_numerada
- Propósito: Ordenar e numerar sequencialmente as transações de cada cliente.
- Entrada: CTE `transacao_filtrada`.
- Lógica: Usa `ROW_NUMBER()` para atribuir uma posição (1, 2, 3...) a cada transação
           de um cliente, ordenada por tempo. É crucial para a recursão.
- Saída: Transações com uma coluna de sequência `rn`.
*/
    transacao_numerada as (
        select
            data,
            hash_cliente,
            datetime_transacao,
            tile_id,
            row_number() over (
                partition by hash_cliente order by datetime_transacao
            ) as rn
        from transacao_filtrada
    ),
    /*
Etapa 7: transacao_destino
- Propósito: Enriquecer cada transação com a localização da transação seguinte.
- Entrada: CTE `transacao_numerada`.
- Lógica: Usa `LEAD(tile_id)` para buscar o `tile_id` da próxima transação do mesmo cliente,
           criando um "destino preditivo".
- Saída: Tabela onde cada linha representa uma origem e já contém a informação de seu destino.
*/
    transacao_destino as (
        select
            data,
            datetime_transacao,
            tile_id as tile_id_origem,
            hash_cliente,
            lead(tile_id) over (
                partition by hash_cliente order by datetime_transacao
            ) as tile_id_destino_preditivo,
            rn
        from transacao_numerada
    ),
    /*
Etapa 8: viagens (CTE Recursiva)
- Propósito: Agrupar sequências de transações em "viagens" com base em regras de tempo.
- Lógica:
  - Membro Âncora: Pega a primeira transação (`rn = 1`) de cada cliente para iniciar a `id_viagem = 1`.
  - Membro Recursivo: Une o passo anterior (`j`) com o passo seguinte (`t`) e aplica duas regras:
    1. Regra de 24h (no `JOIN`): Interrompe a sequência se o gap entre transações for > 24h.
    2. Regra de 4h (no `CASE`): Compara a transação atual com o início da viagem (`datetime_ancora`).
       Se <= 4h, continua na mesma viagem. Se > 4h, inicia uma nova viagem.
- Saída: Tabela onde cada transação está marcada com o `id_viagem` ao qual pertence.
*/
    viagens as (
        select
            data,
            hash_cliente,
            datetime_transacao,
            tile_id_origem,
            tile_id_destino_preditivo,
            rn,
            datetime_transacao as datetime_ancora,
            1 as id_viagem
        from transacao_destino
        where rn = 1
        union all
        select
            t.data,
            t.hash_cliente,
            t.datetime_transacao,
            t.tile_id_origem,
            t.tile_id_destino_preditivo,
            t.rn,
            case
                when datetime_diff(t.datetime_transacao, j.datetime_ancora, hour) <= 4
                then j.datetime_ancora
                else t.datetime_transacao
            end as datetime_ancora,
            case
                when datetime_diff(t.datetime_transacao, j.datetime_ancora, hour) <= 4
                then j.id_viagem
                else j.id_viagem + 1
            end as id_viagem
        from transacao_destino as t
        join
            viagens as j
            on t.hash_cliente = j.hash_cliente
            and t.rn = j.rn + 1
            and datetime_diff(t.datetime_transacao, j.datetime_transacao, hour) <= 24
    ),
    /*
Etapa 9: viagens_agregadas
- Propósito: "Colapsar" as transações de cada viagem em uma única linha de resumo.
- Lógica: Agrupa por `id_viagem` e calcula ID de origem, destino híbrido e tamanho da viagem.
- Saída: Uma linha por viagem com suas características resumidas e essenciais.
*/
    viagens_agregadas as (
        select
            min(data) as data,
            hash_cliente,
            id_viagem,
            array_agg(tile_id_origem order by datetime_transacao asc limit 1)[
                offset(0)
            ] as tile_id_origem,
            coalesce(
                array_agg(
                    tile_id_destino_preditivo order by datetime_transacao desc limit 1
                )[offset(0)],
                array_agg(tile_id_origem order by datetime_transacao desc limit 1)[
                    offset(0)
                ]
            ) as tile_id_destino,
            count(*) as tamanho_viagem
        from viagens
        group by hash_cliente, id_viagem
    ),
    /*
Etapa 10: viagens_avaliadas
- Propósito: Adicionar contexto a cada viagem para permitir a filtragem final.
- Lógica: Usa `MAX(id_viagem)` para identificar qual é a última viagem de cada cliente.
- Saída: Viagens resumidas, onde cada uma sabe qual é a última viagem de seu cliente.
*/
    viagens_avaliadas as (
        select *, max(id_viagem) over (partition by hash_cliente) as max_viagem_cliente
        from viagens_agregadas
    ),
    /*
Etapa 11: viagens_finais
- Propósito: Aplicar a regra de negócio final, enriquecer com dados do calendário e
             SELECIONAR APENAS AS COLUNAS NECESSÁRIAS para a próxima etapa de agregação.
- Lógica:
  1. Filtra com `WHERE`: mantém uma viagem se ela tem 2 ou mais transações,
     OU se ela não é a última viagem do cliente.
  2. Adiciona `tipo_dia` e `subtipo_dia` com um `LEFT JOIN`.
- Otimização: Seleciona apenas as colunas que serão usadas no `GROUP BY` final.
- Saída: A lista final e filtrada de viagens válidas, pronta para a agregação.
*/
    viagens_finais as (
        select j.data, j.tile_id_origem, j.tile_id_destino, c.tipo_dia, c.subtipo_dia
        from viagens_avaliadas as j
        left join calendario as c using (data)
        where {{ incremental_filter }}
        and (j.tamanho_viagem >= 2 or j.id_viagem < j.max_viagem_cliente)
    ),
    /*
Etapa 12: viagens_finais_agg
- Propósito: Gerar o relatório final agregado.
- Lógica: Agrupa por rota (O-D) e tipo de dia, e calcula a média de viagens por dia
           para cada uma dessas combinações.
- Saída: Tabela final com a média diária de viagens por rota e tipo de dia.
*/
    viagens_finais_agg as (
        select
            tipo_dia,
            subtipo_dia,
            tile_id_origem,
            tile_id_destino,
            {# string_agg(
                distinct cast(data as string), ', ' order by data
            ) as datas_consideradas, #}
            count(*) as total_viagens,
            count(distinct data) as total_dias,
        from viagens_finais
        group by all
    )
/*
Etapa 13: Seleção Final
- Propósito: Selecionar os dados finais e enriquecê-los com metadados de execução.
- Lógica: Adiciona colunas fixas ou de variáveis do dbt para governança e rastreabilidade,
           como versão, timestamp da execução e ID da invocação do dbt.
- Saída: A tabela final a ser materializada pelo dbt, contendo os dados e metadados.
*/
select
    tipo_dia,
    subtipo_dia,
    tile_id_origem,
    tile_id_destino,
    {# datas_consideradas, #}
    total_viagens,
    total_dias,
    total_viagens / total_dias as media_viagens_dia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagens_finais_agg
