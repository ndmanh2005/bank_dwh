{% macro decode_status(col, type) -%}

{% if type == 'account_status'-%}
case
when {{col}} = 'A' then 'Active'
when {{col}} = 'C' then 'Closed'
when {{col}} = 'D' then 'Dormant'
when {{col}} = 'F' then 'Frozen'
end
{%- elif type == 'branch_status' -%}
case
when {{col}} = 'A' then 'Active'
when {{col}} = 'C' then 'Closed'
end
{%- elif type == 'customer_type'-%} 
case

when {{col}} = 'I' then 'Cá nhân'
when {{col}} = 'C' then 'Doanh nghiệp'
end
{%- elif type == 'id_type'-%}
case 
when {{col}} = 'CC' then 'CCCD'
when {{col}} = 'PP' then 'Hộ chiếu'
when {{col}} = 'CMT' then 'CMND'
when {{col}} = 'BL' then 'Giấp phép KD'
end
{%- elif type == 'transaction_type'-%}
case 
when {{col}} = 'CR' then 'Credit'
when {{col}} = 'DR' then 'Debit'
end
{%- elif type == 'channel'-%}
case
when {{col}} = 'ATM' then 'POS'
when {{col}} = 'IB' then 'Internet Banking'
when {{col}} = 'MB' then 'Mobile Banking'
when {{col}} = 'COUNTER' then 'Quầy'
end
{%- elif type == 'account_type'-%}
case 
when {{col}} = 'SA' then 'SA'
when {{col}} = 'CA' then 'CA'
when {{col}} = 'TD' then 'TD'
when {{col}} = 'LN' then 'LN'
end
{%- elif type == 'gender'-%}
case
when {{col}} = 'M' then 'Nam'
when {{col}} = 'F' then 'Nữ'
when {{col}} = 'NULL' then 'N/A'
end
{%- else -%}
        /* CẢNH BÁO: Loại decode '{{ type }}' không tồn tại trong hệ thống */
        {{ col }}
{% endif %}
{% endmacro -%}


