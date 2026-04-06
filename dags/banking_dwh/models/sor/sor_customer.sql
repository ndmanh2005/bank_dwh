{{ config(
    materialized='table'
) }}
SELECT
ROW_NUMBER() OVER (ORDER BY customer_id) as customer_key,
customer_id as customer_id,
UPPER(TRIM(customer_name)) as full_name,
CASE
  WHEN gender = 'M' THEN 'Nam'
  WHEN gender = 'F' THEN 'Nữ'
  ELSE 'N/A'
END
as gender_desc,
date_of_birth as date_of_birth,
CASE
  WHEN date_of_birth IS NOT NULL
  THEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth))::INTEGER
  ELSE NULL
END
as age,
id_number as id_number,
CASE
  WHEN id_type = 'CC'  THEN 'CCCD'
  WHEN id_type = 'PP'  THEN 'Hộ chiếu'
  WHEN id_type = 'CMT' THEN 'CMND'
  WHEN id_type = 'BL'  THEN 'Giấy phép KD'
  ELSE 'Khác'
END
as id_type_desc,
phone as phone,
LOWER(TRIM(email)) as email,
CASE
  WHEN customer_type = 'I' THEN 'Cá nhân'
  WHEN customer_type = 'C' THEN 'Doanh nghiệp'
  ELSE 'Khác'
END
as customer_type_desc,
registration_date as registration_date,
CURRENT_DATE as effective_from,
CAST('9999-12-31' AS DATE)
as effective_to,
TRUE as is_current,
CURRENT_TIMESTAMP
as etl_loaded_at





FROM {{ source('stg','stg_customer') }}