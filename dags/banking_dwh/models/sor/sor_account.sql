{{ config(
    materialized='table'
) }}
SELECT
ROW_NUMBER() OVER (ORDER BY a.account_id)
as account_key,
a.account_id,
sc.customer_key
as customer_key,
a.account_number,
a.product_code,
COALESCE(p.product_name, 'Unknown')
as product_name,
{{ decode_status('UPPER(TRIM(a.account_type))', 'account_type') }}
as account_type_desc,
a.currency_code,
COALESCE(p.interest_rate, 0) as interest_rate,
a.open_date,a.close_date,
CASE
  WHEN a.close_date IS NOT NULL
  THEN (a.close_date - a.open_date)
  ELSE (CURRENT_DATE - a.open_date)
END as account_age_days,
{{decode_status ('a.status', 'account_status')}}
as status_desc,
sb.branch_key as branch_key,



sb.branch_name as branch_name,
CURRENT_TIMESTAMP as etl_loaded_at



FROM {{ source('stg','stg_account')}} a
LEFT JOIN {{ source('stg','stg_product')}} p
on a.product_code = p.product_code
INNER JOIN {{ ref("sor_customer") }} sc 
ON a.customer_id = sc.customer_id AND sc.is_current = TRUE
INNER JOIN  {{ ref("sor_branch") }} sb
ON a.branch_id = sb.branch_id