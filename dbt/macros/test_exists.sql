{% macro test_exists(model, column_name) %}

    select count({{ column_name }})
    from {{ model }}
    having 1=0
    limit 1

{% endmacro %}
