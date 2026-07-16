-- Rule: discount là %, phải nằm trong khoảng 0 đến 1 (0% đến 100%)
-- discount âm hoặc > 100% là lỗi nhập liệu
SELECT *
FROM {{ ref('fct_sales') }}
WHERE discount < 0 or discount > 1