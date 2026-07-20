SELECT
    PRODUCT_CODE as id,
    PRODUCT as product_name,
    CATEGORY as category,
    SUBCATEGORY as sub_category
FROM
    {{ source('e_commerce', 'product') }}  