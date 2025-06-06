SELECT
  table_catalog AS project_id,
  table_schema AS dataset_id,
  table_name AS table_id,
  column_name,
  data_type,
  description
FROM
  rj-smtr.`region-US`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS