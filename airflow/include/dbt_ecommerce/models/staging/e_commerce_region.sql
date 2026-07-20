SELECT
    REGION_CODE as id,
    CITY as city,
    STATE as state,
    COUNTRY as country,
    COUNTRY_LATITUDE as country_latitude,
    COUNTRY_LONGITUDE as country_longitude,
    REGION as region,
    MARKET as market
FROM
    {{ source('e_commerce', 'region') }}