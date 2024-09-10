# -*- coding: utf-8 -*-
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StringType
from shapely import wkt
from shapely.geometry import LineString, Point


def cut(line, distance, lines):
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [LineString(coords[: i + 1]), LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            lines.append(LineString(coords[:i] + [(cp.x, cp.y)]).wkt)
            line = LineString([(cp.x, cp.y)] + coords[i:])
            if line.length > distance:
                cut(line, distance, lines)
            else:
                lines.append(LineString([(cp.x, cp.y)] + coords[i:]).wkt)
            return lines


def cut_udf(wkt_string):
    line = wkt.loads(wkt_string)
    return cut(line, distance=1000, lines=list())


cut_udf = udf(cut_udf, ArrayType(StringType()))


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    df = dbt.ref("aux_shapes_wkt")

    df_segments = df.withColumn("segments", cut_udf(col("shape_wkt")))
    df_exploded = df_segments.select(explode(col("segments")).alias("segment"))
    return df_exploded
