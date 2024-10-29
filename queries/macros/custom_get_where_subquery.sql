{% macro get_where_subquery(relation) -%}
    {% set where = config.get('where') %}
    {% if where %}
        {% if "__date_range_start__" in where %}
            {# replace placeholder string with result of custom macro #}
            {% set date_range_start = var('date_range_start') %}
            {% set where = where | replace("__date_range_start__", date_range_start) %}
        {% endif %}
        {% if "__date_range_end__" in where %}
            {# replace placeholder string with result of custom macro #}
            {% set date_range_end = var('date_range_end') %}
            {% set where = where | replace("__date_range_end__", date_range_end) %}
        {% endif %}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}