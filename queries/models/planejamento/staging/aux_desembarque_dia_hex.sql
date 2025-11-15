/*
Documentação do Modelo
======================================================

1. Propósito do Modelo
-----------------------
Este modelo serve como um passo preparatório para a calibração duplamente restrita
da matriz Origem-Destino (Método de Furness). Como não possuímos dados observados
reais de desembarques, este modelo sintetiza uma estimativa para eles.

A sua função é calcular o total de viagens que **chegam** a cada hexágono de destino,
com base na matriz de viagens que já foi expandida pelos totais de **saída** (embarques).
O resultado serve como o "alvo" ou "restrição" para as colunas durante o processo
de balanceamento da matriz no modelo Python subsequente.

2. Fontes de Dados (Entradas)
-----------------------------
- `{{ ref("aux_matriz_origem_destino_expandida_origem") }}`: A matriz OD que foi
  previamente expandida para corresponder aos totais reais de embarques em cada
  zona de origem (matriz singelamente restrita).

3. Lógica de Negócio (Passo a Passo)
--------------------------------------
1. **Seleção de Dados:** A consulta lê a matriz expandida por origem. Cada linha
   nesta tabela representa um fluxo de viagens de um `origem_id` para um `destino_id`.
2. **Agregação por Destino:** A lógica agrupa todas as linhas pelo hexágono de
   **destino** (`destino_id`), bem como pelo `tipo_dia` e `subtipo_dia`.
3. **Soma dos Fluxos:** Para cada grupo (cada hexágono de destino em um tipo de dia),
   a função `SUM(viagens_expandidas_dia)` calcula o total de viagens que chegaram
   àquele local, vindas de todas as origens.
4. **Resultado:** O valor agregado é nomeado `quantidade_desembarques_estimada`,
   deixando claro que é um resultado do modelo anterior, e não um dado real observado.

4. Estrutura da Saída (Colunas)
-------------------------------
- `destino_id`: O identificador único do hexágono H3 de destino.
- `tipo_dia`: O tipo de dia (ex: 'Dia Útil', 'Sábado').
- `subtipo_dia`: O subtipo de dia (ex: 'Normal', 'Feriado').
- `quantidade_desembarques_estimada`: A soma total de viagens expandidas que
  chegaram a este hexágono neste tipo de dia.

5. Materialização
-----------------
- `table`: O resultado é salvo como uma tabela física para ser consumida de forma
  eficiente pelo modelo Python de calibração.

*/

{{
    config(
        materialized="table",
    )
}}

SELECT
    destino_id,
    tipo_dia,
    subtipo_dia,
    SUM(viagens_expandidas_dia) AS quantidade_desembarques_estimada
FROM
    {{ ref('aux_matriz_origem_destino_expandida_origem') }}
GROUP BY
    ALL