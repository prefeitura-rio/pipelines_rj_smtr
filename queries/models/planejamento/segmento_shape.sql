{{
  config(
    partition_by = {
      'field' :'feed_start_date',
      'data_type' :'date',
      'granularity': 'day'
    },
    tags=['geolocalizacao']
  )
}}