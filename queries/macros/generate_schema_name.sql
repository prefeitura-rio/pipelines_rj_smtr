-- fmt: off
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {% set schema_name = default_schema %}

    {%- else -%}

        {% set schema_name = custom_schema_name | trim %}

    {%- endif -%}

    {% if target.name == "dev" %}
        {% set schema_name = env_var("DBT_USER") + "__" + schema_name %}
    {% endif %}

    {{ schema_name }}
{%- endmacro %}
