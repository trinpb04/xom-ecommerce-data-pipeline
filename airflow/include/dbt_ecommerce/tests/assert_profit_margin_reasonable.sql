-- Rule: profit_margin vượt quá ±100% là bất thường (lời/lỗ nhiều hơn cả giá trị đơn hàng)
-- không hẳn là SAI tuyệt đối, nhưng đáng để nhìn qua nếu test này fail
SELECT *
FROM {{ ref('fct_sales') }}
WHERE profit_margin > 1 or profit_margin < -1