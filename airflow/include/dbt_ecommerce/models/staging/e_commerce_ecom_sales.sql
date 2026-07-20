SELECT
    ROW_ID as row_id,
    ORDER_ID as order_id,
    ORDER_DATE as order_date,
    CUSTOMER_ID as customer_id,
    SEGMENT as segment,
    REGEXP_REPLACE(REGION_CODE, '^R+', 'R') as region_id,
    REGEXP_REPLACE(PRODUCT_CODE, '^P+', 'P') as product_id,
    QUANTITY as quantity,
    SALES as sales,
    DISCOUNT as discount,
    PROFIT as profit
FROM
    {{ source('e_commerce', 'ecom_sales') }}