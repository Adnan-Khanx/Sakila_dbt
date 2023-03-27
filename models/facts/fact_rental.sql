{{config(materialized='incremental',unique_key='rental_id')}}

with rental_base as(
    SELECT *,
    EXTRACT(EPOCH from rental_date::timestamp) as rental_epoch,
    EXTRACT(EPOCH from return_date::timestamp) as return_epoch,
    EXTRACT(EPOCH from return_date::timestamp)  - EXTRACT(EPOCH from rental_date::timestamp)  as diff,
    CASE WHEN return_date IS NOT NULL THEN 1 ELSE 0 END AS IS_RETURN,
    TO_CHAR(rental_date::timestamp,'YYYYMMDD')::INTEGER AS DATE_KEY,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S")}}' as DBT_TIME
    FROM
    {{source('stg','rental')}}

),
inventory as(
    SELECT * FROM {{source('stg','inventory')}}
),
dim_film as(
    SELECT * from {{ref('dim_film')}}
),
dim_store as(
    SELECT * from {{ref('dim_store')}}
),
dim_staff as(
    SELECT * from {{ref('dim_staff')}}
),
dim_customer as(
    SELECT * from {{ref('dim_customer')}}
),
rental_base_1 as(
    SELECT rental_base.*,
    inventory.store_id,
    inventory.film_id
    FROM
    rental_base 
    LEFT JOIN inventory
    ON rental_base.inventory_id = inventory.inventory_id
),
rental_base_2 as(
    SELECT rental_base_1.*,
    CASE WHEN dim_staff.staff_id IS NOT NULL THEN dim_staff.staff_id ELSE -1 END AS STAFF_ID_RENTAL_CHECK,
    CASE WHEN dim_customer.CUSTOMER_id IS NOT NULL THEN dim_customer.CUSTOMER_id ELSE -1 END AS CUSTOMER_ID_CHECK,
    CASE WHEN dim_film.film_id IS NOT NULL THEN dim_film.film_id ELSE -1 END AS FILM_ID_CHECK,
    CASE WHEN dim_store.STORE_ID IS NOT NULL THEN dim_store.STORE_ID ELSE -1 END AS STORE_ID_CHECK
    FROM
    rental_base_1

    LEFT JOIN dim_staff
    ON dim_staff.staff_id=rental_base_1.staff_id

    LEFT JOIN dim_customer
    ON rental_base_1.CUSTOMER_ID=dim_customer.CUSTOMER_ID

    LEFT JOIN dim_film
    ON rental_base_1.film_id = dim_film.film_id

    LEFT JOIN dim_store
    ON rental_base_1.STORE_ID= dim_store.STORE_ID
)

SELECT
    rental_id,
    rental_date,
    DATE_KEY,
    inventory_id,
    CUSTOMER_ID_CHECK as customer_id,
    FILM_ID_CHECK as film_id,
    STORE_ID_CHECK as store_id,
    STAFF_ID_RENTAL_CHECK as staff_id,
    return_date,
    CASE WHEN return_date is NOT NULL then diff/3600 else NULL end as rental_hours,
    IS_RETURN,
    last_update,
    DBT_TIME

FROM
    rental_base_2
    where 1=1
    {% if is_incremental() %}
    and last_update::timestamp > (SELECT max(last_update) from {{this}})
    {% endif %}