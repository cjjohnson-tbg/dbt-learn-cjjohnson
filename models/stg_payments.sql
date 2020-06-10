SELECT
    id as payment_id,
    orderid as order_id,
    created as payment_on,
    (amount/100)::decimal(19,2) as payment_amount
    
FROM raw.stripe.payment
WHERE status = 'success'