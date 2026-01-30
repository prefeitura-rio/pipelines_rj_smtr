{% test test_completude_temperatura(model) %}
    with
        limite_datas as (
            select
                date('{{ var("date_range_start") }}') as data_inicio,
                if(
                    date_diff(
                        date('{{ var("date_range_end") }}'),
                        date('{{ var("date_range_start") }}'),
                        day
                    )
                    = 1,
                    date_sub(date('{{ var("date_range_end") }}'), interval 1 day),
                    date('{{ var("date_range_end") }}')
                ) as data_fim
        ),
        datas_esperadas as (
            select data
            from
                limite_datas, unnest(generate_date_array(data_inicio, data_fim)) as data
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
