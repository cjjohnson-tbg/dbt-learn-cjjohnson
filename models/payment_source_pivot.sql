{% set methods = ['credit_card','coupon','bank_transfer','gift_card'] %}

with payments as (
    select * from {{ ref('stg_payments') }}
)


    select
        order_id,
        {%- for method in methods %}
        sum(case when payment_method = '{{ method }}' then payment_amount else 0 end) as {{ method }}_amount 
            {%- if not loop.last %}, {% endif %}
        {%- endfor %} 
    from payments
    group by 1