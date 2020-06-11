with payments as (
    select * from {{ ref('stg_payments') }}
)
    select
        order_id,
        SUM(case when payment_method = 'credit_cart' then amount else 0 end) as credit_card_amount,
        SUM(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        SUM(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        SUM(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount
    from payments
    group by 1