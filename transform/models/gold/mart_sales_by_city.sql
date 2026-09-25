with joined_orders as (
    select * from {{ ref('int_orders_joined') }}
),

aggregated as (
    select
        customer_city,
        customer_state,
        count(order_id) as total_orders
    from joined_orders
    group by 1, 2
)

select * from aggregated
order by total_orders desc
