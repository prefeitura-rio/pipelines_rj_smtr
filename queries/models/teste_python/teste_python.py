# -*- coding: utf-8 -*-
import pyproj
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StringType
from shapely import wkt
from shapely.geometry import LineString, Point
from shapely.ops import transform


def cut(line, distance, lines):
    wgs84 = pyproj.CRS("EPSG:4326")
    utm = pyproj.CRS("EPSG:31983")
    project = pyproj.Transformer.from_crs(utm, wgs84, always_xy=True).transform
    line = wkt.loads(line)
    if distance <= 0.0 or distance >= line.length:
        return [transform(project, LineString(line)).wkt]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                transform(project, LineString(coords[: i + 1])).wkt,
                transform(project, LineString(coords[i:])).wkt,
            ]
        if pd > distance:
            cp = line.interpolate(distance)
            lines.append(transform(project, LineString(coords[:i] + [(cp.x, cp.y)])).wkt)
            line = LineString([(cp.x, cp.y)] + coords[i:])
            if line.length > distance:
                cut(line.wkt, distance, lines)
            else:
                lines.append(transform(project, LineString([(cp.x, cp.y)] + coords[i:])).wkt)
            return lines


def cut_udf(wkt_string):
    wgs84 = pyproj.CRS("EPSG:4326")
    utm = pyproj.CRS("EPSG:31983")
    project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
    wkt_string = transform(project, wkt.loads(wkt_string)).wkt
    return cut(wkt_string, distance=1000, lines=list())


cut_udf = udf(cut_udf, ArrayType(StringType()))


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    df = dbt.ref("aux_shapes_wkt")

    df_segments = df.withColumn("segments", cut_udf(col("shape_wkt")))
    df_exploded = df_segments.select(
        "shape_id", "feed_start_date", explode(col("segments")).alias("segment")
    )

    return df_exploded
