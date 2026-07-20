with summary as (
    SELECT * FROM {{ ref('int_customer_order_summary') }}
),

-- "Hôm nay" của tập dữ liệu = ngày đơn hàng cuối cùng có trong DATA,
-- không dùng current_date() vì data bán hàng là data lịch sử
reference_date as (
    select max(last_order_date) as ref_date from summary
),

final as (
    SELECT
        s.customer_id,
        s.first_order_date,
        s.last_order_date,
        datediff(day, s.last_order_date, r.ref_date) as days_since_last_order,
        s.total_orders,
        s.total_net_sales,
        s.total_profit,

        -- returning customer: đơn giản, không cần framework phức tạp -
        -- chỉ cần biết khách mua nhiều hơn 1 lần hay chỉ 1 lần rồi thôi
        case when s.total_orders > 1 then true else false end as is_returning_customer
    FROM summary s
    cross join reference_date r
)

SELECT * FROM final