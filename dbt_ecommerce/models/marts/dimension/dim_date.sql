with spine as (
    {{ date_spine_for_ecom_sales() }}
)

SELECT
    cast(date_day as date) as date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    'Q' || extract(quarter from date_day) as quarter_name,
    extract(month from date_day) as month,
    to_char(date_day, 'Mon') as month_name
FROM
    spine