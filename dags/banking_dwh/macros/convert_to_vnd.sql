{%- macro convert_to_vnd(amount_col, currency_col, rate_col) -%}
case 
when {{currency_col}} = 'VND' then {{amount_col}}
else {{amount_col}} * COALESCE({{rate_col}},0)
end
{% endmacro -%}

{% macro get_exchange_rate(currency_col, rate_col) %}
case 
when {{currency_col}} = 'VND' then 1
else {{rate_col}}
end
{% endmacro %}

{%- macro build_exchange_rate_join(alias, date_col, currency_col) -%}
left join
{{source('stg','stg_exchange_rate')}} as {{alias}}
on {{alias}}.currency_from = {{currency_col}}
and {{alias}}.currency_to = 'VND'
and {{alias}}.rate_date = {{date_col}}
{%- endmacro -%}
