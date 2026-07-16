-- rule: net_sales (sau khi trừ discount) không bao giờ lớn hơn sales (giá gốc)
-- Nếu fail: công thức tính sai, hoặc discount đang bị lưu là số âm
SELECT *
FROM {{ ref('fct_sales') }}
WHERE net_sales > sales