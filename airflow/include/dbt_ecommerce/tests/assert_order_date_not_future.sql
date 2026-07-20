-- Rule: order_date không được lớn hơn ngày hiện tại - dấu hiệu lỗi nhập liệu hoặc lỗi ETL
SELECT *
FROM {{ ref('fct_sales') }}
WHERE order_date > current_date()