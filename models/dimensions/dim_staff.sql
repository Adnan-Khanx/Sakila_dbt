{{config(materialized='incremental',unique_key='staff_id',post_hook='insert into {{this}}(staff_id) VALUES (-1)') }}

with staff_base as(
    SELECT *,
    ACTIVE::INT as ACTIVE_INT,
    CASE WHEN ACTIVE::INT=0 then 'no' else 'yes' end as ACTIVE_DESC,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S")}}' as DBT_TIME
    FROM
    {{ source('stg','staff')}} 

)

SELECT 
    staff_id,
    FIRST_NAME,
    last_name,
    email,
    ACTIVE_INT as active,
    ACTIVE_DESC,
    last_update,
    DBT_TIME

FROM
    staff_base

    
WHERE 1=1 

{% if is_incremental %}
and last_update > (SELECT max(last_update) from {{this}})
{% endif %}