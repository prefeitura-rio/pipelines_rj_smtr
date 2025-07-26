# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd


def model(dbt, session):
    """
    Documentação do Modelo: validacao_convergencia_calibracao
    ======================================================
    1. Propósito
    ------------
    Este modelo calcula o Erro Quadrático Médio (RMSE) para avaliar a convergência
    do processo de calibração. Ele mede a diferença entre os totais de embarque/desembarque
    na matriz final calibrada e os totais reais que foram usados como alvo.

    2. Entradas
    -----------
    - `matriz_origem_destino_calibrada`: O resultado final do modelo de calibração.
    - `aux_embarques_reais_por_hex`: Totais reais de embarques.
    - `aux_desembarques_reais_por_hex`: Totais reais (ou estimados) de desembarques.

    3. Saída
    --------
    Tabela no formato (metrica, valor, descricao) com os valores de RMSE.
    """
    dbt.config(materialized="table")

    # Carregar os modelos como DataFrames
    matriz_calibrada = dbt.ref("matriz_origem_destino_calibrada")
    alvos_origem = dbt.ref("aux_embarques_reais_por_hex")
    alvos_destino = dbt.ref("aux_desembarques_reais_por_hex")

    # Calcular somas finais da matriz calibrada por tipo de dia
    somas_finais_origem = (
        matriz_calibrada.groupby(["tipo_dia", "subtipo_dia", "origem_id"])["viagens_calibradas_dia"]
        .sum()
        .reset_index()
    )
    somas_finais_destino = (
        matriz_calibrada.groupby(["tipo_dia", "subtipo_dia", "destino_id"])[
            "viagens_calibradas_dia"
        ]
        .sum()
        .reset_index()
    )

    # Juntar totais finais com os alvos para calcular o erro
    erros_origem = pd.merge(
        somas_finais_origem, alvos_origem, on=["tipo_dia", "subtipo_dia", "origem_id"], how="outer"
    ).fillna(0)

    erros_destino = pd.merge(
        somas_finais_destino,
        alvos_destino,
        on=["tipo_dia", "subtipo_dia", "destino_id"],
        how="outer",
    ).fillna(0)

    # Calcular RMSE
    rmse_origens = np.sqrt(
        np.mean(
            (erros_origem["viagens_calibradas_dia"] - erros_origem["quantidade_embarques_real"])
            ** 2
        )
    )
    rmse_destinos = np.sqrt(
        np.mean(
            (
                erros_destino["viagens_calibradas_dia"]
                - erros_destino["quantidade_desembarques_estimada"]
            )
            ** 2
        )
    )

    # Criar o DataFrame de saída
    output_df = pd.DataFrame(
        [
            {
                "metrica": "rmse_convergencia_origens",
                "valor": round(rmse_origens, 4),
                "descricao": "Erro Quadrático Médio da aderência aos totais de embarque. Próximo de zero indica ótima convergência.",
            },
            {
                "metrica": "rmse_convergencia_destinos",
                "valor": round(rmse_destinos, 4),
                "descricao": "Erro Quadrático Médio da aderência aos totais de desembarque. Próximo de zero indica ótima convergência.",
            },
        ]
    )

    return output_df
