{% test test_completude_temperatura(model) %}

    with
        validation as (
            select data, count(distinct hora) as qtd
            from {{ model }}
            where
                id_estacao in (
                    'A621',
                    'A652',
                    'A636',
                    'A602',
                    '1',
                    '11',
                    '16',
                    '19',
                    '20',
                    '22',
                    '28',
                    '32'
                )
                and temperatura is not null
            group by data
        )

    select *
    from validation
    where qtd != 24

{% endtest %}
