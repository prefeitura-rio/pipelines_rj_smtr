{% test test_completude_temperatura(model) %}
    with
        datas_esperadas as (
            select data
            from
                unnest(
                    generate_date_array(
                        date('{{ var("date_range_start") }}'),
                        date('{{ var("date_range_end") }}')
                    )
                ) as data
        ),
        validation as (
            select data, count(distinct hora) as qtd
            from {{ model }}
            where temperatura is not null
            group by data
        )
    select e.data, v.qtd
    from datas_esperadas e
    left join validation v using (data)
    where v.qtd is null or v.qtd != 24
{% endtest %}
