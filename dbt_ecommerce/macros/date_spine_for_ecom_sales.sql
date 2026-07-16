{% macro date_spine_for_ecom_sales() %}

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select min(order_date) from " ~ ref('e_commerce_ecom_sales') ~ ")",
        end_date="(select dateadd(day, 1, max(order_date)) from " ~ ref('e_commerce_ecom_sales') ~ ")"
    ) }}

{% endmacro %}