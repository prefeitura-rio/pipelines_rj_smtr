{% test test_completude_temperatura(model) %}

    with
        validation as (
            select data, count(distinct hora) as qtd
            from {{ model }}
            where temperatura is not null
            group by data
        )

    select *
    from validation
    where qtd != 24

{% endtest %}
