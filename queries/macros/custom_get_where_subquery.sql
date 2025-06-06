{% macro get_where_subquery(relation) -%}
    {% set where = config.get("where") %}
    {% if where %}
        {% set re = modules.re %}
        {% set final_where = namespace(value=where) %}

        {% set matches = re.finditer("\{([^}]+)\}", where) %}

        {% for match in matches %}
            {% set var_name = match.group(1) %}
            {% set var_value = var(var_name) %}
            {% set var_replace = "{" ~ var_name ~ "}" %}
            {% set final_where.value = final_where.value.replace(
                var_replace, var_value
            ) %}
        {% endfor %}
        (select * from {{ relation }} where {{ final_where.value }})
    {%- else -%} {{ relation }}
    {%- endif -%}
{%- endmacro %}
