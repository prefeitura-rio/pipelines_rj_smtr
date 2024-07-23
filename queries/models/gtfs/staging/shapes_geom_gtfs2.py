import pandas as pd
import geopandas as gpd
import shapely

def model(dbt, session):

    print(dbt)
    print(session)

    dbt.config(partition_by = { 'field' :'feed_start_date',
                               'data_type' :'date',
                               'granularity': 'day' }
    )
    dbt.config(unique_key = ['shape_id', 'feed_start_date'])
    dbt.config(alias = 'shapes_geom2')
    dbt.config(packages = ["geopandas","shapely","pandas"])

    shapes_df_spk = dbt.ref("shapes_gtfs")
    feed_info_df_spk = dbt.ref("feed_info_gtfs")
    data_versao_gtfs = dbt.config.get("data_versao_gtfs")
    last_feed_version= dbt.config.get("data_versao_gtfs")
    version = dbt.config.get("version")

    shapes_df = shapes_df_spk.toPandas()
    feed_info_df = feed_info_df_spk.toPandas()

    # Assuming 'shapes_df' is a Spark DataFrame
    # point_udf = pyspark.sql.functions.udf(lambda lon, lat: shapely.Point(lon, lat))

    # shapes_df = shapes_df.withColumn(
    #     'ponto_shape',
    #     point_udf(shapes_df['shape_pt_lon'], shapes_df['shape_pt_lat'])
    # )

    # Convert to GeoDataFrame
    shapes_df['ponto_shape'] = shapes_df[['shape_pt_lon', 'shape_pt_lat']].apply(lambda row: shapely.Point(row['shape_pt_lon'], row['shape_pt_lat']), axis=1)
    gdf = gpd.GeoDataFrame(shapes_df, geometry='ponto_shape')

    if dbt.is_incremental:
        gdf = gdf[gdf['feed_start_date'].isin([last_feed_version, data_versao_gtfs])]
    
    # Contents
    contents = gdf[['shape_id', 'ponto_shape', 'shape_pt_sequence', 'feed_start_date']]
    
    # PTS
    contents['final_pt_sequence'] = contents.groupby(['feed_start_date', 'shape_id'])['shape_pt_sequence'].transform('max')
    pts = contents.sort_values(by=['feed_start_date', 'shape_id', 'shape_pt_sequence'])
    
    # Shapes
    shapes = pts.groupby(['shape_id', 'feed_start_date']).agg({
        'ponto_shape': lambda x: shapely.LineString(x.tolist()),
        'ponto_shape': ['first', 'last']
    }).reset_index()
    shapes.columns = ['shape_id', 'feed_start_date', 'shape', 'start_pt', 'end_pt']
    
    # Shapes Half
    def create_half_shapes(df, condition):
        return df[df['shape_pt_sequence'] <= condition]

    half_0 = pts.groupby(['shape_id', 'feed_start_date']).apply(lambda df: create_half_shapes(df, df['final_pt_sequence'].iloc[0] // 2))
    half_1 = pts.groupby(['shape_id', 'feed_start_date']).apply(lambda df: create_half_shapes(df, df['final_pt_sequence'].iloc[0] // 2 + 1))
    
    half_0['new_shape_id'] = half_0['shape_id'] + "_0"
    half_1['new_shape_id'] = half_1['shape_id'] + "_1"
    
    shapes_half_0 = half_0.groupby(['shape_id', 'feed_start_date', 'new_shape_id']).agg({
        'ponto_shape': lambda x: shapely.LineString(x.tolist()),
        'ponto_shape': ['first', 'last']
    }).reset_index()
    shapes_half_1 = half_1.groupby(['shape_id', 'feed_start_date', 'new_shape_id']).agg({
        'ponto_shape': lambda x: shapely.LineString(x.tolist()),
        'ponto_shape': ['first', 'last']
    }).reset_index()
    
    shapes_half_0.columns = ['shape_id', 'feed_start_date', 'new_shape_id', 'shape', 'start_pt', 'end_pt']
    shapes_half_1.columns = ['shape_id', 'feed_start_date', 'new_shape_id', 'shape', 'start_pt', 'end_pt']
    
    shapes_half = pd.concat([shapes_half_0, shapes_half_1], axis=0)
    
    # IDs
    ids = shapes.groupby(['feed_start_date', 'shape_id']).first().reset_index()
    
    # Union Shapes
    union_shapes = pd.concat([ids, shapes_half], axis=0, ignore_index=True)
    
    union_shapes = union_shapes[(round(union_shapes['start_pt'].y, 4) == round(union_shapes['end_pt'].y, 4)) &
                                (round(union_shapes['start_pt'].x, 4) == round(union_shapes['end_pt'].x, 4))]
    
    # Final Selection
    result = union_shapes.merge(feed_info_df, on='feed_start_date')
    
    if dbt.is_incremental:
        result = result[result['feed_start_date'].isin([last_feed_version, data_versao_gtfs])]
    
    result['shape_distance'] = result['shape'].apply(lambda x: round(x.length, 1))
    result['versao_modelo'] = version
    
    final_columns = ['feed_version', 'feed_start_date', 'feed_end_date', 'shape_id', 'shape', 'shape_distance', 'start_pt', 'end_pt', 'versao_modelo']
    result = result[final_columns]

    output_df = session.create_dataframe(result)
    
    return output_df