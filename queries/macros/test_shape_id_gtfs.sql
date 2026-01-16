{% test test_shape_id_gtfs(model, column_name) %}--fmt:off
    select distinct--fmt:on
        t.feed_start_date, t.shape_id
    from {{ ref("trips_gtfs") }} t
    left join {{ ref("shapes_gtfs") }} s using (feed_start_date, shape_id)
    where
        s.shape_id is null
        and t.feed_start_date = '{{ var("data_versao_gtfs") }}'--fmt:off
{% endtest %}
