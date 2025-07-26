import pandas as pd
from pandas import DataFrame

def model(dbt, session: "bigquery") -> DataFrame:
    """
    Documentação do Modelo: matriz_origem_destino_calibrada
    ======================================================

    1. Propósito do Modelo
    -----------------------
    Este modelo realiza a calibração completa de uma matriz Origem-Destino (OD) expandida.
    Utilizando o Método de Furness (um algoritmo de IPF - Iterative Proportional Fitting),
    ele ajusta iterativamente a matriz para que ela seja "duplamente restrita". Isso
    significa que a soma das viagens que saem de cada origem e a soma das viagens que
    chegam em cada destino correspondem aos totais reais conhecidos (ground truth).
    O resultado é uma matriz OD final, balanceada e mais precisa.

    2. Fontes de Dados (Entradas)
    -----------------------------
    - `{{ ref("aux_matriz_origem_destino_expandida_origem") }}`: A matriz OD "semente", já
      expandida para corresponder aos totais de embarques (restringida por origem).
    - `{{ ref("aux_embarques_reais_por_hex") }}`: Tabela com os totais reais de EMBARQUES
      (produções) por hexágono e tipo de dia. Usado como o alvo para a soma das linhas.
    - `{{ ref("aux_desembarques_reais_por_hex") }}`: Tabela com os totais reais de DESEMBARQUES
      (atrações) por hexágono e tipo de dia. Usado como o alvo para a soma das colunas.

    3. Lógica de Negócio (Passo a Passo)
    --------------------------------------
    1. **Carregamento de Dados:** Os três modelos de entrada são carregados como DataFrames
       do pandas.
    2. **Loop por Segmento:** A calibração é executada separadamente para cada combinação
       de `tipo_dia` e `subtipo_dia` para garantir a precisão dos resultados.
    3. **Pivotagem:** Para cada segmento, a matriz de viagens (que está em formato "longo")
       é pivotada para o formato de matriz "larga" (origens nas linhas, destinos nas colunas).
    4. **Alinhamento:** Os índices (hexágonos de origem) e colunas (hexágonos de destino)
       da matriz e dos vetores de totais são alinhados para garantir consistência.
    5. **Calibração Iterativa (Método de Furness):** Um loop é executado (ex: 10 vezes)
       para ajustar alternadamente a matriz:
        a. As linhas são escaladas para que sua soma seja igual aos embarques totais.
        b. As colunas são escaladas para que sua soma seja igual aos desembarques totais.
    6. **Despivotagem e Consolidação:** Após a calibração de cada segmento, a matriz
       "larga" é transformada de volta para o formato "longo" e os resultados de todos
       os segmentos são combinados.

    4. Estrutura da Saída (Colunas)
    -------------------------------
    - `tipo_dia`, `subtipo_dia`, `origem_id`, `destino_id`: Identificadores da rota e do dia.
    - `viagens_calibradas_dia`: O número de viagens do par O-D após o processo de
      calibração completo.

    """
    # Configura o dbt para materializar o resultado como uma tabela
    dbt.config(materialized="table")

    # 1. Carregar os dados de entrada usando referências do dbt
    matriz_expandida_df = dbt.ref("aux_matriz_origem_destino_expandida_origem")
    totais_origem_df = dbt.ref("aux_embarque_dia_hex")
    totais_destino_df = dbt.ref("aux_desembarque_dia_hex")

    # Lista para armazenar os resultados calibrados de cada segmento
    resultados_calibrados = []

    # Identificar os segmentos únicos para iterar (ex: dia útil, sábado, etc.)
    segmentos = matriz_expandida_df[["tipo_dia", "subtipo_dia"]].drop_duplicates().to_records(index=False)

    # 2. Loop para calibrar cada segmento separadamente
    for tipo_dia, subtipo_dia in segmentos:
        
        # Filtra os dados para o segmento atual
        matriz_segmento = matriz_expandida_df[
            (matriz_expandida_df["tipo_dia"] == tipo_dia) &
            (matriz_expandida_df["subtipo_dia"] == subtipo_dia)
        ]
        
        alvos_origem_segmento = totais_origem_df[
            (totais_origem_df["tipo_dia"] == tipo_dia) &
            (totais_origem_df["subtipo_dia"] == subtipo_dia)
        ].set_index("origem_id")["quantidade_embarques_real"]

        alvos_destino_segmento = totais_destino_df[
            (totais_destino_df["tipo_dia"] == tipo_dia) &
            (totais_destino_df["subtipo_dia"] == subtipo_dia)
        ].set_index("destino_id")["quantidade_desembarques_real"]

        # Se não houver dados para este segmento, pula para o próximo
        if matriz_segmento.empty:
            continue

        # 3. Preparar os dados para a iteração (pivotar a matriz)
        matriz_pivotada = matriz_segmento.pivot_table(
            index="origem_id", columns="destino_id", values="viagens_expandidas_dia"
        ).fillna(0)

        # 4. Alinhar os índices e colunas para garantir a correspondência
        matriz_pivotada, alvos_origem = matriz_pivotada.align(alvos_origem_segmento, axis=0, fill_value=0)
        matriz_pivotada, alvos_destino = matriz_pivotada.align(alvos_destino_segmento, axis=1, fill_value=0)

        # 5. Implementar o loop iterativo do Método de Furness (ex: 10 iterações)
        for _ in range(10):
            # Evitar divisão por zero, substituindo 0 por 1 no divisor
            soma_linhas = matriz_pivotada.sum(axis=1)
            soma_linhas[soma_linhas == 0] = 1
            fatores_linha = alvos_origem.divide(soma_linhas)
            matriz_pivotada = matriz_pivotada.multiply(fatores_linha, axis=0)

            soma_colunas = matriz_pivotada.sum(axis=0)
            soma_colunas[soma_colunas == 0] = 1
            fatores_coluna = alvos_destino.divide(soma_colunas)
            matriz_pivotada = matriz_pivotada.multiply(fatores_coluna, axis=1)

        # 6. Preparar o DataFrame final (transformar de volta para o formato longo)
        df_calibrado_segmento = matriz_pivotada.stack().reset_index()
        df_calibrado_segmento.columns = ["origem_id", "destino_id", "viagens_calibradas_dia"]
        
        # Adicionar as colunas de segmento de volta
        df_calibrado_segmento["tipo_dia"] = tipo_dia
        df_calibrado_segmento["subtipo_dia"] = subtipo_dia
        
        # Adicionar o resultado à lista
        resultados_calibrados.append(df_calibrado_segmento)

    # 7. Concatenar os resultados de todos os segmentos
    df_final_calibrado = pd.concat(resultados_calibrados)
    
    # Opcional: Remover linhas com fluxo zero para manter a tabela menor
    df_final_calibrado = df_final_calibrado[df_final_calibrado["viagens_calibradas_dia"] > 0.01]

    # 8. Retornar o DataFrame final para o dbt materializar
    return df_final_calibrado