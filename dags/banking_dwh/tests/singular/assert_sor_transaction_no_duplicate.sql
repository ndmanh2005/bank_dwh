select 
transaction_id,
count(transaction_id)
from
{{ref('sor_transaction')}}
group by transaction_id having count(*) > 1

