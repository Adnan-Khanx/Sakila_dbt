{{config(materialized='incremental',unique_key='customer_id',post_hook='insert into {{this}}(customer_id) VALUES (-1)') }}

with customer_base as(
    SELECT *,
    CONCAT(CONCAT(customer.FIRST_NAME,' '),customer.LAST_NAME)  AS FULL_NAME,
    SUBSTRING(customer.EMAIL FROM POSITION('@' IN customer.EMAIL)+1 FOR CHAR_LENGTH(customer.EMAIL)-POSITION('@' IN EMAIL)) AS DOMAIN,
    customer.active::int as ACTIVE_INT,
    CASE WHEN customer.ACTIVE=0 then 'no' else 'yes' end as ACTIVE_DESC,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S")}}' as DBT_TIME
    FROM
    {{ source('stg','customer')}} as customer


),
address as (
    SELECT * FROM
    {{ source('stg','address')}}

),
city as (
    SELECT * FROM
    {{ source('stg','city')}}

),
country as (
    SELECT * FROM
    {{ source('stg','country')}}

)


SELECT 
customer_base.CUSTOMER_ID,
customer_base.STORE_ID,
customer_base.FIRST_NAME,
customer_base.LAST_NAME,
customer_base.FULL_NAME,
customer_base.EMAIL,
customer_base.DOMAIN,
customer_base.ACTIVE_INT AS ACTIVE,
customer_base.ACTIVE_DESC,
customer_base.create_date,
customer_base.last_update,
customer_base.DBT_TIME,

address.ADDRESS_ID::INT,
address.address,
city.city_id,
city.city,
country.country_id,
country.country


FROM customer_base

LEFT JOIN ADDRESS AS address
	ON customer_base.address_id= address.address_id

LEFT JOIN CITY AS city
	ON address.city_id=city.city_id

LEFT JOIN COUNTRY AS country
	ON country.country_id=city.country_id

WHERE 1=1 

{% if is_incremental %}
and customer_base.last_update > (SELECT max(last_update) from {{this}})
{% endif %}