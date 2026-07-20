with sales as (
    SELECT * from {{ ref('e_commerce_ecom_sales') }}
),

product as (
    SELECT * from {{ ref('e_commerce_product') }}
),

region as (
    SELECT * from {{ ref('e_commerce_region') }}
),

enriched as (
    SELECT
        sales.row_id,
        sales.order_id,
        sales.order_date,
        sales.customer_id,
        sales.segment as customer_segment,
        sales.region_id,
        region.region,
        region.market,
        region.country,
        sales.product_id,
        product.category,
        product.sub_category,
        sales.quantity,
        sales.sales,
        sales.discount,
        sales.profit,

        -- net_sales: doanh thu THỰC nhận sau khi trừ chiết khấu.
        -- ĐÃ XÁC NHẬN bằng query kiểm tra unit_price (sales/quantity) theo product_code:
        -- unit_price KHÔNG đổi bất kể discount -> SALES là giá gốc CHƯA trừ discount.
        -- Vậy phải nhân (1-discount) ở đây mới ra doanh thu thực nhận.
        -- ví dụ: sales=100, discount=0.1 (10%) -> net_sales = 100 * (1-0.1) = 90
        sales.sales * (1 - coalesce(sales.discount, 0)) as net_sales,

        -- profit_margin: lợi nhuận / doanh thu.
        CASE 
            WHEN sales.sales != 0 THEN sales.profit / sales.sales 
            ELSE NULL 
        END as profit_margin,

        -- unit_price: giá bán mỗi đơn vị TRƯỚC discount - dùng so sánh giá giữa sản phẩm/category
        CASE
            WHEN sales.quantity != 0 THEN round(sales.sales / sales.quantity, 2)
            ELSE NULL
        END as unit_price,

        -- discount_amount: số tiền $ THỰC TẾ đã giảm - khác cột discount (chỉ là %)
        round(sales.sales * coalesce(sales.discount, 0), 2) as discount_amount,

        -- is_profitable: cờ nhanh lọc giao dịch lời/lỗ, không cần so profit từng lần
        CASE WHEN sales.profit > 0 THEN true ELSE false END as is_profitable

    FROM sales
    LEFT JOIN product
        on sales.product_id = product.id
    LEFT JOIN region
        on sales.region_id = region.id
)

SELECT * FROM enriched