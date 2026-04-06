

WITH stg_count AS (
    SELECT COUNT(*) AS cnt 
    FROM {{ source('stg', 'stg_customer') }}
),

sor_count AS (
    SELECT COUNT(*) AS cnt 
    FROM {{ ref('sor_customer') }}
    WHERE is_current = TRUE
)

SELECT 
    stg.cnt AS stg_count, 
    sor.cnt AS sor_count, 
    (stg.cnt - sor.cnt) AS difference,
    'Lệch số lượng khách hàng: Nguồn STG có ' || stg.cnt || ' người, nhưng đích SOR đang active ' || sor.cnt || ' người.' AS error_message
FROM stg_count stg
CROSS JOIN sor_count sor
WHERE stg.cnt <> sor.cnt