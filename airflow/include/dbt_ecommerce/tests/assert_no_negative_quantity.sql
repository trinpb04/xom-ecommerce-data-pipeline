-- Rule: không được có dòng bán với quantity <= 0 (đơn hàng không thể bán 0 hoặc âm sản phẩm)
SELECT
    *
FROM
    {{ ref('fct_sales') }}
WHERE
    quantity <= 0