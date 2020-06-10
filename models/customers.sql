{{
  config(
    materialized='view'
  )
}}

with customers as (
    select
        customer_id,
        first_name,
        last_name
    from {{ ref('stg_customers')}}
),
orders as (
    select 
        order_id,
        customer_id,
        order_date
        from {{ ref('stg_orders')}}
),
order_payment_amount as (
    SELECT 
        order_id,
        sum(payment_amount) as order_amount
    FROM {{ ref('stg_payments')}}
    GROUP BY order_id
),
customer_orders as (
    select
        customer_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders,
        SUM(order_payment_amount.order_amount) as lifetime_value

    from orders 
    LEFT OUTER JOIN order_payment_amount on orders.order_id = order_payment_amount.order_id
    group by 1

),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value,0) as lifetime_value
    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id

)

select * from final