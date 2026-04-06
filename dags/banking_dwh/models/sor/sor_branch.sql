{{ config(
    materialized='table'
) }}



SELECT
{{generate_surrogate_key(['branch_id'])}} as branch_key,
branch_id,
UPPER(branch_name) as branch_name,
branch_code,
region,
city,
TRIM(address) as address,
manager_name,
{{decode_status('status', 'branch_status')}}
as status_desc,
{{ etl_timestamp() }} as etl_loaded_at

FROM {{ source('stg', 'stg_branch') }}