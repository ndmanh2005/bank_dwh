{% test count_match(model, column_name, source_table, source_column) %}

    SELECT {{ source_column }}
    FROM {{ source_table }}

    EXCEPT

    SELECT {{ column_name }}
    FROM {{ model }}

{% endtest %}