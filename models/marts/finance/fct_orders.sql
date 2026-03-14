with customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),


orders as(
    select * from {{ ref('fct_orders') }}
),

order_payments as(
    select 
    customer_id,
    min (order_date) as first_order_date,
    max (order_date) as most_recent_order_date,
    count(order_id) as nunber_of_orders
    sum(case when payment_status = 'success' then payment_amount end) as amount

    from payment
    group by 1
),

final as (
    select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    coalesce (order_payments.amount, 0) as amount
    from orders
    left join order_payments using (order_id)
)

select * from final