/*
Documentação do Modelo
======================================================

1. Propósito do Modelo
-----------------------
Este modelo expande uma matriz Origem-Destino (OD) "bruta", que foi gerada a partir
de uma amostra de passageiros identificados. A expansão utiliza os totais reais de
embarques por hexágono para escalar proporcionalmente os fluxos de viagem, resultando
em uma matriz que representa o universo total de passageiros, incluindo os não
identificados (ex: pagantes em dinheiro).

Este processo é tecnicamente conhecido como um "Modelo de Crescimento Singelamente
Restrito", pois garante que a soma das viagens que saem de cada origem na matriz
final seja igual ao total real de embarques conhecido para aquela origem.

2. Fontes de Dados (Entradas)
-----------------------------
- `{{ ref("aux_matriz_origem_destino_bruta") }}`: A matriz OD inicial, contendo os fluxos
  de viagem observados (inferidos) da amostra de passageiros com bilhetagem eletrônica.
- `{{ ref("aux_passageiro_dia_hex") }}`: Tabela que contém a contagem "real" e total
  de embarques por hexágono, para todo o universo de passageiros.

3. Lógica de Negócio (Passo a Passo)
--------------------------------------
1. **`embarques_reais`:** Primeiro, a consulta agrega os dados de embarques totais para
   obter uma contagem consolidada de embarques para cada hexágono (`tile_id`) no
   período de análise. Este é o nosso "total verdadeiro" ou "ground truth".

2. **`embarques_observados_por_origem`:** Em seguida, a consulta calcula o total de
   viagens que saem de cada hexágono de origem com base apenas na matriz bruta.
   Isso nos dá o total "observado" ou "inferido" da nossa amostra.

3. **`fatores_de_expansao`:** O passo crucial. Esta CTE calcula o "Fator de Expansão"
   para cada hexágono de origem, dividindo o total real de embarques (da Etapa 1)
   pelo total observado de viagens (da Etapa 2). A função `SAFE_DIVIDE` é usada para
   evitar erros caso um hexágono não tenha viagens observadas.

4. **`SELECT` Final:** A consulta final une a matriz bruta original com os fatores de
   expansão correspondentes a cada origem. Ela então multiplica a quantidade de
   viagens observadas de cada par O-D pelo fator de expansão de sua origem para
   calcular o valor final (`viagens_expandidas_origem`).

4. Estrutura da Saída (Colunas)
-------------------------------
- `tipo_dia`, `subtipo_dia`, `origem_id`, `destino_id`: Identificadores da rota e do dia.
- `viagens_observadas_dia`: O número de viagens do par O-D na amostra bruta.
- `viagens_expandidas_dia`: O número de viagens do par O-D após a expansão,
  representando o fluxo total estimado.

*/

WITH
-- Etapa 1: Sua matriz de viagens observadas (o resultado do modelo anterior)
matriz_bruta AS (
    SELECT * FROM {{ ref("aux_matriz_origem_destino_bruta") }}
),

-- Etapa 2: A contagem real de embarques por hexágono (sua nova fonte de dados)
embarques_reais AS (
    SELECT
        tile_id,
        SUM(quantidade_embarques) AS quantidade_embarques_real
    FROM {{ ref("aux_embarque_dia_hex") }}
    GROUP BY tile_id
),

-- Etapa 3: Calcular o total de embarques OBSERVADOS em sua matriz bruta para cada origem
embarques_observados_por_origem AS (
    SELECT
        origem_id,
        SUM(total_viagens) AS total_viagens_observadas
    FROM matriz_bruta
    GROUP BY origem_id
),

-- Etapa 4: Calcular o Fator de Expansão para cada hexágono de origem
fatores_de_expansao AS (
    SELECT
        obs.origem_id,
        SAFE_DIVIDE(real.quantidade_embarques_real, obs.total_viagens_observadas) AS fator_expansao
    FROM embarques_observados_por_origem AS obs
    JOIN embarques_reais AS real
        ON obs.origem_id = real.tile_id
)

-- Etapa Final: Aplicar o fator de expansão à matriz bruta
SELECT
    bruta.tipo_dia,
    bruta.subtipo_dia,
    bruta.origem_id,
    bruta.destino_id,
    -- {# bruta.datas_consideradas, #} -- Coluna intencionalmente comentada
    bruta.total_viagens AS viagens_observadas_dia,
    bruta.total_viagens * COALESCE(fator.fator_expansao, 0) AS viagens_expandidas_dia
FROM matriz_bruta AS bruta
LEFT JOIN fatores_de_expansao AS fator
    ON bruta.origem_id = fator.origem_id