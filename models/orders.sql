with orders as (
    SELECT 
        order_id,
        customer_id,
        status,
        order_date
    FROM {{ ref('stg_orders')}}
),
order_payments as (
    SELECT
        order_id,
        sum(payment_amount) as amount
    FROM {{ ref('stg_payments')}}
    GROUP BY order_id
)
, final as (
    SELECT
    orders.order_id,
    orders.customer_id,
    order_payments.amount
    FROM orders 
    LEFT OUTER JOIN order_payments on orders.order_id = order_payments.order_id
)

SELECT * FROM final order by final.order_id