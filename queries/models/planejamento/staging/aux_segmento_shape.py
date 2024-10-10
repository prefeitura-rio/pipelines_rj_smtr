# -*- coding: utf-8 -*-
import numpy as np
import pyproj
from pyspark.sql.functions import col, explode, udf
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


def cut(line, distance):
    line_len = line.length

    dist_mod = line_len % distance
    dist_range = list(np.arange(0, line_len, distance))
    middle_index = len(dist_range) // 2

    last_final_dist = 0
    lines = []

    for i, _ in enumerate(dist_range, start=1):
        if i == middle_index:
            cut_distance = dist_mod
        else:
            cut_distance = distance
        final_dist = last_final_dist + cut_distance
        lines.append(
            [str(i), transform_projection(substring(line, last_final_dist, final_dist), True).wkt]
        )
        last_final_dist = final_dist

    return lines


def cut_udf(wkt_string):
    line = transform_projection(wkt.loads(wkt_string))
    return cut(line, distance=1000)


cut_udf = udf(cut_udf, ArrayType(ArrayType(StringType())))


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    df = dbt.ref("aux_shapes_geom_filtrada")

    df_segments = df.withColumn("shape_lists", cut_udf(col("wkt_shape")))

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
        .drop("shape_list")
    )

    return df_exploded
