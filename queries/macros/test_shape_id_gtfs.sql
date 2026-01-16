{% test test_shape_id_gtfs(model, column_name) %}

    select t.shape_id
    from {{ ref("trips_gtfs") }} t
    left join {{ ref("shapes_gtfs") }} s on t.shape_id = s.shape_id
    where t.feed_start_date = s.feed_start_date and s.shape_id is null

{% endtest %}
