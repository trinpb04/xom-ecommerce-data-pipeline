with customer AS (
    select * from {{ ref('e_commerce_customer') }}
),

final as (
    SELECT
        id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        birth_date,
        datediff(year, birth_date, current_date()) as age,
        marital_status,
        gender,
        email,
        annual_income,
        edu_level,
        occupation,
        home_owner
    FROM customer
)

select * from final