# -*- coding: utf-8 -*-

# ----------------------------------------------------------------
# IMPORTAÇÃO DE BIBLIOTECAS
# ----------------------------------------------------------------
# Bibliotecas padrão e geoespaciais
import numpy as np
import pandas as pd
import pyproj
from geovoronoi import voronoi_regions_from_coords
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from shapely import wkt
from shapely.geometry import box
from shapely.ops import transform


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
        packages=["numpy", "pandas", "pyproj", "shapely", "geovoronoi"],
    )

    # --- 1. SETUP E LEITURA DOS DADOS PREPARADOS ---

    # Lê a tabela de paradas já filtrada e georreferenciada.
    df_prepared_stops = dbt.ref("aux_stops_gtfs")

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
    stops_pandas_df = df_prepared_stops.select(
        "representative_stop_id", "wkt_representative_stop"
    ).toPandas()

    if stops_pandas_df.empty:
        return df_prepared_stops.withColumn("wkt_voronoi", lit(None).cast(StringType()))

    # Converte WKT para objetos Shapely e projeta para o sistema métrico
    stops_pandas_df["geometry_metric"] = stops_pandas_df["wkt_representative_stop"].apply(
        lambda w: transform_projection(wkt.loads(w))
    )

    points_coords = np.array(
        stops_pandas_df["geometry_metric"].apply(lambda p: (p.x, p.y)).tolist()
    )

    if len(points_coords) < 4:
        return df_prepared_stops.withColumn("wkt_voronoi", lit(None).cast(StringType()))

    # Cria bounding box com margem
    minx, miny = points_coords.min(axis=0)
    maxx, maxy = points_coords.max(axis=0)
    buffer_x = (maxx - minx) * 0.05
    buffer_y = (maxy - miny) * 0.05
    geographic_box = box(minx - buffer_x, miny - buffer_y, maxx + buffer_x, maxy + buffer_y)

    # --- Cálculo de Voronoi com clipping automático ---
    regions, _ = voronoi_regions_from_coords(points_coords, geographic_box)

    # voronoi_polygons = {}
    # for point_idx, poly in regions.items():
    #     if poly.is_valid and not poly.is_empty:
    #         final_polygon = transform_projection(poly, from_shapely=True)
    #         representative_stop_id = stops_pandas_df.iloc[point_idx]["representative_stop_id"]
    #         voronoi_polygons[representative_stop_id] = final_polygon.wkt

    # Cria DataFrame com índice dos pontos e geometria Voronoi
    regions_df = (
        pd.DataFrame.from_dict(regions, orient="index", columns=["geometry_metric"])
        .reset_index()
        .rename(columns={"index": "point_idx"})
    )

    # Junta com a tabela original de paradas, para recuperar o stop_id
    regions_df["representative_stop_id"] = regions_df["point_idx"].map(
        stops_pandas_df["representative_stop_id"]
    )

    # Transforma a geometria de volta para EPSG:4326
    regions_df["geometry_wgs84"] = regions_df["geometry_metric"].apply(
        lambda g: transform_projection(g, from_shapely=True)
    )

    # Extrai WKT
    regions_df["wkt_voronoi"] = regions_df["geometry_wgs84"].apply(lambda g: g.wkt)

    # --- 3. JUNÇÃO DOS RESULTADOS E RETORNO ---

    # DataFrame com polígonos
    # voronoi_df_pandas = pd.DataFrame(
    #     voronoi_polygons.items(), columns=["representative_stop_id", "wkt_voronoi"]
    # )

    # --- 3. JUNÇÃO DOS RESULTADOS E RETORNO ---

    # Converte o DataFrame de resultados de Pandas para Spark
    voronoi_df_spark = session.createDataFrame(regions_df)

    # Junta os polígonos de Voronoi de volta ao DataFrame original das paradas
    final_df = df_prepared_stops.join(voronoi_df_spark, on="representative_stop_id", how="left")

    # Retorna o DataFrame final enriquecido
    return final_df
