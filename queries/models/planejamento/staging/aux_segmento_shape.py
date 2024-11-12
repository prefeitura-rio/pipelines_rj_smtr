# -*- coding: utf-8 -*-
import numpy as np
import pyproj
from pyspark.sql.functions import col, explode, lit, udf
from pyspark.sql.types import ArrayType, StringType
from shapely import wkt

# from shapely.geometry import LineString, Point
from shapely.ops import substring, transform


def transform_projection(shape, from_utm=False):
    bq_projection = pyproj.CRS("EPSG:4326")
    shapely_projection = pyproj.CRS("EPSG:31983")
    if from_utm:
        project = pyproj.Transformer.from_crs(
            shapely_projection, bq_projection, always_xy=True
        ).transform
    else:
        project = pyproj.Transformer.from_crs(
            bq_projection, shapely_projection, always_xy=True
        ).transform

    return transform(project, shape)


# calcular distancia e criar buffer aqui
def cut(line, distance, buffer_size):
    line_len = line.length

    dist_mod = line_len % distance
    dist_range = list(np.arange(0, line_len, distance))
    middle_index = (len(dist_range) // 2) + 1

    last_final_dist = 0
    lines = []

    for i, _ in enumerate(dist_range, start=1):
        if i == middle_index:
            cut_distance = dist_mod
        else:
            cut_distance = distance
        final_dist = last_final_dist + cut_distance
        segment = substring(line, last_final_dist, final_dist)
        lines.append(
            [
                str(i),
                transform_projection(segment, True).wkt,
                segment.length,
                transform_projection(segment.buffer(distance=buffer_size), True).wkt,
            ]
        )
        last_final_dist = final_dist

    return lines


def cut_udf(wkt_string, buffer_size):
    line = transform_projection(wkt.loads(wkt_string))
    return cut(line, distance=1000, buffer_size=buffer_size)


cut_udf = udf(cut_udf, ArrayType(ArrayType(StringType())))


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    df = dbt.ref("aux_shapes_geom_filtrada")

    df_segments = df.withColumn(
        "shape_lists",
        cut_udf(col("wkt_shape"), lit(dbt.config.get("buffer_segmento_metros"))),
    )

    df_exploded = (
        df_segments.select(
            "feed_version",
            "feed_start_date",
            "feed_end_date",
            "shape_id",
            explode(col("shape_lists")).alias("shape_list"),
        )
        .withColumn("id_segmento", col("shape_list").getItem(0))
        .withColumn("wkt_segmento", col("shape_list").getItem(1))
        .withColumn("comprimento_segmento", col("shape_list").getItem(2))
        .withColumn("buffer_completo", col("shape_list").getItem(3))
        .drop("shape_list")
    )

    return df_exploded
