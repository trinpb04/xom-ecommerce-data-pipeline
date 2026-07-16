SELECT
    id,
    product_name,
    category,
    sub_category
FROM
    {{ ref('e_commerce_product') }}         