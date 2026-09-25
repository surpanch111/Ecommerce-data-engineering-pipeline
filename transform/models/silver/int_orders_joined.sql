with orders as (
    -- Need to create stg_orders first ideally, but referencing source or staging works.
    -- Assuming a stg_orders model exists mirroring stg_customers.
    select * from {{ source('ecommerce_raw', 'orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from joined
