{% test value_in_range(model, column_name, min_value, max_value) %}
    select {{column_name}}, 'Lỗi biên độ: Giá trị của cột ' || '{{ column_name }}' || ' nằm ngoài khoảng [' || '{{ min_value }}' || ' đến ' || '{{ max_value }}' || ']' AS ly_do_vi_pham
    from {{model}}
    where {{column_name}} is not null
    and ({{column_name}} < {{min_value}} or {{column_name}} > {{max_value}})
    {% endtest %}