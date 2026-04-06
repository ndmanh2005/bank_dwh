{% macro generate_surrogate_key(column_names) %}
    MD5(CONCAT_WS('|',
        {%- for col in column_names %}
            COALESCE(CAST({{ col }} AS TEXT), 'NULL')
            {%- if not loop.last %}, {% endif -%}
        {% endfor -%}
    ))
{% endmacro %}