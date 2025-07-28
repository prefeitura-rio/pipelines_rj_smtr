# -*- coding: utf-8 -*-

# ----------------------------------------------------------------
# IMPORTAÇÃO DE BIBLIOTECAS
# ----------------------------------------------------------------
# Bibliotecas padrão e geoespaciais
import numpy as np
import pandas as pd
import geopandas as gpd
import pyproj
from scipy.spatial import Voronoi
from shapely import wkt
from shapely.geometry import Polygon, box
from shapely.ops import transform

# Bibliotecas do PySpark
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType


# ----------------------------------------------------------------
# DEFINIÇÃO DO MODELO DBT
# ----------------------------------------------------------------
def model(dbt, session):
    """
    Gera polígonos de Voronoi para cada ponto de ônibus.

    Este modelo dbt em Python consome os dados de paradas de ônibus já filtrados e
    preparados por um modelo upstream (ex: 'aux_stops_gtfs').

    Metodologia:
    1.  **Coleta**: Dados de paradas e da caixa delimitadora são coletados para o driver.
    2.  **Cálculo**: As geometrias são transformadas para uma projeção métrica (SIRGAS 2000)
        para cálculos precisos. O algoritmo de Voronoi é executado e os polígonos
        infinitos são tratados através de um 'clipping' na caixa delimitadora fornecida.
    3.  **Junção**: Os polígonos de Voronoi resultantes são unidos ao conjunto de
        dados original, enriquecendo cada parada com sua área de influência.

    Inputs:
    - dbt.ref("aux_stops_gtfs")
    - dbt.ref(dbt.config.get("limites_caixa"))

    Output: Spark DataFrame com todas as colunas do input + a coluna 'wkt_voronoi'.
    """

    # --- Configuração do Modelo dbt ---
    dbt.config(
        materialized="table",
        packages=["numpy", "pandas", "geopandas", "pyproj", "shapely", "scipy"]
    )

    # --- 1. SETUP E LEITURA DOS DADOS PREPARADOS ---

    # Lê a tabela de paradas já filtrada e georreferenciada.
    df_prepared_stops = dbt.ref("aux_stops_gtfs")

    # Lê a tabela que contém a caixa delimitadora
    df_box_limits = dbt.ref("aux_limites_caixa")

    # Definição das projeções CRS (Coordinate Reference System)
    bq_projection = pyproj.CRS(dbt.config.get("projecao_wgs_84"))
    shapely_projection = pyproj.CRS(dbt.config.get("projecao_sirgas_2000"))

    def transform_projection(shape, from_shapely=False):
        """Função auxiliar para transformar a projeção de uma geometria Shapely."""
        if from_shapely:
            project = pyproj.Transformer.from_crs(
                shapely_projection, bq_projection, always_xy=True
            ).transform
        else:
            project = pyproj.Transformer.from_crs(
                bq_projection, shapely_projection, always_xy=True
            ).transform
        return transform(project, shape)

    # --- 2. CÁLCULO GLOBAL DE VORONOI NO DRIVER ---

    # Coleta os dados para Pandas DataFrames para computação em memória.
    stops_pandas_df = df_prepared_stops.select("stop_id", "wkt_stop").toPandas()
    box_limits_pandas_df = df_box_limits.toPandas()

    if stops_pandas_df.empty or box_limits_pandas_df.empty:
        return df_prepared_stops.withColumn("wkt_voronoi", lit(None).cast(StringType()))

    # Converte WKT para objetos Shapely e projeta para o sistema métrico
    stops_pandas_df["geometry_metric"] = stops_pandas_df["wkt_stop"].apply(
        lambda w: transform_projection(wkt.loads(w))
    )

    points_coords = np.array(
        stops_pandas_df["geometry_metric"].apply(lambda p: (p.x, p.y)).tolist()
    )

    if len(points_coords) < 4:
        return df_prepared_stops.withColumn("wkt_voronoi", lit(None).cast(StringType()))

    # Executa o algoritmo de Voronoi
    vor = Voronoi(points_coords)

    # AJUSTE PRINCIPAL: Usa a caixa delimitadora fornecida pelo dbt var
    # Extrai os limites do DataFrame da caixa
    box_row = box_limits_pandas_df.iloc[0]
    # Cria a geometria da caixa em coordenadas geográficas
    geographic_box = box(
        box_row['min_longitude'],
        box_row['min_latitude'],
        box_row['max_longitude'],
        box_row['max_latitude']
    )
    # Transforma a caixa para a mesma projeção métrica dos cálculos
    bounding_box_metric = transform_projection(geographic_box)

    # Itera sobre as regiões de Voronoi para construir os polígonos
    voronoi_polygons = {}
    for i, region_index in enumerate(vor.point_region):
        if -1 not in vor.regions[region_index]:
            polygon_vertices = vor.vertices[vor.regions[region_index]]
            polygon = Polygon(polygon_vertices)

            # Recorta ("clipa") o polígono com a caixa delimitadora externa
            clipped_polygon = polygon.intersection(bounding_box_metric)

            if clipped_polygon.is_valid and not clipped_polygon.is_empty:
                # Transforma o polígono de volta para a projeção geográfica (WGS 84)
                final_polygon = transform_projection(clipped_polygon, from_shapely=True)

                # Associa o polígono ao seu 'stop_id' original
                stop_id = stops_pandas_df.iloc[i]['stop_id']
                voronoi_polygons[stop_id] = final_polygon.wkt

    # Cria um novo DataFrame Pandas com os resultados
    voronoi_df_pandas = pd.DataFrame(
        voronoi_polygons.items(), columns=["stop_id", "wkt_voronoi"]
    )

    # --- 3. JUNÇÃO DOS RESULTADOS E RETORNO ---

    # Converte o DataFrame de resultados de Pandas para Spark
    voronoi_df_spark = session.createDataFrame(voronoi_df_pandas)

    # Junta os polígonos de Voronoi de volta ao DataFrame original das paradas
    final_df = df_prepared_stops.join(
        voronoi_df_spark,
        on="stop_id",
        how="left"
    )

    # Retorna o DataFrame final enriquecido
    return final_df