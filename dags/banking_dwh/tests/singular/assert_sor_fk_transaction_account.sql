select
child.account_key as error_key,
'mã giao dịch ' || child.account_key || 'khong có account tương ứng' as error_message
from
{{ ref('sor_account')}} parent
right join
{{ref('sor_transaction')}} child
on child.account_key = parent.account_key
where child.account_key is not null
and parent.account_key is null
union all

select
child.customer_key as error_key,
'mã giao dịch ' || child.customer_key || 'khong có customer tương ứng' as error_message
from
{{ref('sor_customer')}} parent
right join
{{ref('sor_transaction')}} child
on child.customer_key=parent.customer_key
where
child.customer_key is not null
and parent.customer_key is null
union all

select
child.account_key as error_key,
'daily_balance ' || child.account_key || 'không có account tương ứng' as error_message
from
{{ref('sor_daily_balance')}} child
left join
{{ref('sor_account')}} parent
on child.account_key = parent.account_key
where parent.account_key IS NULL
AND child.account_key IS NOT NULL