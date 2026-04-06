{%- macro etl_timestamp() -%}
CURRENT_TIMESTAMP
{% endmacro %}

{%- macro etl_batch_date() -%}
CURRENT_DATE
{% endmacro %}

{%- macro safe_divide(numerator, denominator, default_val = NULL) -%}
case
when {{denominator }} = 0 or is null then default_val
when {{denominator}} <> 0 then {{numerator}} / {{denominator}}
{% endmacro %}

{% macro coalesce_zero(col) %}
COALESCE(col,0)
{% endmacro %}

{% macro log_model_start(model_name) %}
{{log('>>> đang compile: ' ~ model_name,info=True)}}
{% endmacro %}