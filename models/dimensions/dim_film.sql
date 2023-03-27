{{config(materialized='incremental',unique_key='film_id',post_hook='insert into {{this}}(film_id) VALUES (-1)') }}

with film_base as(
    SELECT 
    film_id,
    title,
    description,
    release_year,
    language_id,
    COALESCE(original_language_id,0) as original_language_id,
    rental_duration,
    rental_rate,
    length,
    replacement_cost,
    rating,
    last_update,
    special_features,
    CASE WHEN POSITION('Trailers' in special_features::varchar)>0 then 1 else 0 end as has_trailers,
    CASE WHEN POSITION('Commentaries' in special_features::varchar)>0 then 1 else 0 end as has_Commentaries,
    CASE WHEN POSITION('Deleted Scenes' in special_features::varchar)>0 then 1 else 0 end as has_Deleted_Scenes,
    CASE WHEN POSITION('Behind the Scenes' in special_features::varchar)>0 then 1 else 0 end as has_Behind_the_Scenes,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S")}}' as DBT_TIME FROM {{source('stg','film')}}

),

language as(
    SELECT * FROM {{source('stg','language')}}
),

film_category as(
    SELECT * FROM {{source('stg','film_category')}}
),

category as(
    SELECT * FROM {{source('stg','category')}}
),

stg_film_1 as(
SELECT 
    film_base.*,
    language.name as lang_name
FROM
    film_base
left join
    language
    on film_base.language_id=language.language_id

),
stg_film_2 as(
    SELECT
    stg_film_1.*,
    film_category.category_id,
    category.name as category_name

    FROM 
    stg_film_1
    LEFT JOIN film_category 
    ON stg_film_1.film_id= film_category.film_id

    LEFT JOIN category
    ON category.category_id = film_category.category_id
)

SELECT *
FROM stg_film_2
where 1=1
{% if is_incremental %}
and last_update > (SELECT max(last_update) from {{this}})
{% endif %}