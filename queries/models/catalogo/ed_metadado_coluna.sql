{% if execute %}
  {% set models_with_tag = get_models_with_tags(["geolocalizacao", "identificacao"]) %}
  {% do log("Models: \n", info=true) %}
  {% for model in models_with_tag %}
    {% do log(model.schema~"."~model.alias~"\n", info=true) %}
  {% endfor %}
{% endif %}

SELECT
  *
FROM
  {{ ref("metadado_coluna") }}
WHERE
  {% for model in models_with_tag %}
    {% if not loop.first %}OR {% endif %}(dataset_id = "{{ model.schema }}"
    AND table_id = "{{ model.alias }}")
  {% endfor %}

  OR (dataset_id = "br_rj_riodejaneiro_stpl_gps"
    AND table_id = "registros")