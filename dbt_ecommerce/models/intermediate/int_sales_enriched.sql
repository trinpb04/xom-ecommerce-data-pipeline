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
        END as profit_margin

    FROM sales
    LEFT JOIN product
        on sales.product_id = product.id
    LEFT JOIN region
        on sales.region_id = region.id
)

SELECT * FROM enriched