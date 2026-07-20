SELECT
    CUSTOMER_ID as id,
    FIRST_NAME AS first_name,
    LAST_NAME as last_name,
    BIRTH_DATE as birth_date,
    MARITAL_STATUS as marital_status,
    GENDER as gender,
    EMAIL_ADDRESS as email,
    ANNUAL_INCOME as annual_income,
    EDUCATION_LEVEL as edu_level,
    OCCUPATION as occupation,
    HOME_OWNER as home_owner
FROM
    {{ source('e_commerce', 'customer') }}  