with sales as (
    select * from {{ ref('int_sales_enriched') }}
),

per_customer as (
    SELECT
        customer_id,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        count(DISTINCT order_id) as total_orders,
        SUM(net_sales) as total_net_sales,
        sum(profit) as total_profit
    FROM
        sales
    GROUP BY customer_id
)

SELECT * FROM per_customer