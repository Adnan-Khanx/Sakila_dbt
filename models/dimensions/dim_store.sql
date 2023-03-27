{{config(materialized='incremental',unique_key='store_id',post_hook='insert into {{this}}(store_id) VALUES (-1)') }}

with stg_store as (
    SELECT *,'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S")}}' as DBT_TIME FROM {{source('stg','store')}}
),

staff as(
    SELECT * FROM {{ref('dim_staff')}}
),

address as (
    SELECT * FROM {{ source('stg','address')}}
),

city as (
    SELECT * FROM {{ source('stg','city')}}
),

country as (
    SELECT * FROM {{ source('stg','country')}}
),

stg_store_1 as(
    SELECT 
    stg_store.*,
    staff.FIRST_NAME AS STAFF_FIRST_NAME,
    staff.last_name AS STAFF_LAST_NAME
    FROM
    stg_store
    left join staff 
    on stg_store.manager_Staff_id = staff.staff_id
),

stg_store_2 as (
    SELECT
    stg_store_1.*,
    address.address,
    city.city_id,
    city.city,
    country.country_id,
    country.country
    FROM
    stg_store_1
    left join address
    on stg_store_1.address_id =address.address_id

    left join city
    on address.city_id= city.city_id

    left join country
    on city.country_id=country.country_id

)

SELECT
    store_id,
    manager_Staff_id,
    STAFF_FIRST_NAME,
    STAFF_LAST_NAME,
    address_id,
    address,
    city_id,
    city,
    country_id,
    country,
    last_update,
    DBT_TIME
FROM
    stg_store_2

WHERE 1=1 

{% if is_incremental %}
and last_update > (SELECT max(last_update) from {{this}})
{% endif %}