{{ config(
    materialized='table'
) }}
SELECT
{{generate_surrogate_key(['t.transaction_id'])}} as transaction_key,
t.transaction_id,
sa.account_key as account_key,
a.account_number as account_number,
sa.customer_key as customer_key,
t.transaction_date as transaction_date,
{{ decode_status('t.transaction_type', 'transaction_type')}}
as transaction_type_desc,
t.amount as amount,
{{convert_to_vnd('t.amount', 't.currency_code', 'er.mid_rate')}} as amount_vnd,
t.currency_code as currency_code,

{{get_exchange_rate('t.currency_code','er.mid_rate')}} as exchange_rate,
t.description as description,

{{ decode_status('t.channel', 'channel') }} as channel_desc,
t.reference_number as reference_number,
{{ etl_timestamp() }} as etl_loaded_at



FROM
{{ source('stg','stg_transaction')}} t
inner JOIN
{{ source('stg','stg_account')}} a
on t.account_id = a.account_id

inner join {{ ref("sor_account")}} sa
ON t.account_id = sa.account_id
{{build_exchange_rate_join('er', 't.transaction_date', 't.currency_code')}}