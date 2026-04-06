{{ config(
    materialized='table'
) }}
SELECT
ROW_NUMBER() OVER (ORDER BY c.customer_key)
as summary_key,
c.customer_key as customer_key,
c.customer_id as customer_id,
c.full_name as full_name,
COUNT(DISTINCT a.account_key) as total_accounts,
COUNT(DISTINCT CASE WHEN a.status_desc = 'Active' THEN a.account_key END)
as total_active_accounts,
COUNT(DISTINCT CASE WHEN a.status_desc = 'Closed' THEN a.account_key END)
as total_closed_accounts,
SUM(db.closing_balance_vnd) as total_balance_vnd,
AVG(db.closing_balance_vnd) as avg_balance_vnd,
SUM(CASE WHEN t.transaction_type_desc = 'Credit' THEN t.amount ELSE 0 END) as total_credit_amount_vnd,
SUM(CASE WHEN t.transaction_type_desc = 'Debit' THEN t.amount ELSE 0 END)
as total_debit_amount_vnd,
COUNT(DISTINCT t.transaction_key)
as total_transactions,
MIN(a.open_date)
as earliest_open_date,
MAX(t.transaction_date)
as latest_transaction_date,
(select a2.branch_name
from {{ ref("sor_account")}} a2
where a2.customer_key=c.customer_key
group by a2.branch_name
order by count(*) desc
limit 1)
as primary_branch_name,
CURRENT_TIMESTAMP
as etl_loaded_at

from
{{ ref("sor_customer")}} c
left join {{ ref("sor_account")}} a
ON c.customer_key = a.customer_key
left join {{ ref("sor_transaction")}} t
ON c.customer_key = t.customer_key
left join (
    /*Lấy số dư trong ngày mới nhất của từng tài khoản*/
    SELECT 
        account_key, 
        closing_balance_vnd
    FROM (
        SELECT 
            account_key, 
            closing_balance_vnd,
            /*Xếp hạng theo ngày, ngày gần nhất (DESC) sẽ có rn = 1*/
            ROW_NUMBER() OVER(PARTITION BY account_key ORDER BY balance_date DESC) AS rn
        FROM {{ ref("sor_daily_balance") }}
    ) sub_balance
    WHERE rn = 1
) db
    ON a.account_key = db.account_key
WHERE c.is_current = TRUE
GROUP BY 
    c.customer_key, 
    c.customer_id, 
    c.full_name