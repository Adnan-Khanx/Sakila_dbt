{{config(materialized='incremental',unique_key='payment_id')}}


SELECT *,'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S")}}' as DBT_TIME
from {{source('stg','payment')}}
where 1=1

{% if is_incremental() %}
and payment_date > (SELECT max(payment_date) from {{this}})
{% endif %}