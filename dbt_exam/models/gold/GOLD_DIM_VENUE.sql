{{
    config(
        materialized='table'
    )
}}


with clened_venue as(
	select
		distinct
		venue_id,
		trim(lower(venue_name)) as venue_name,
		trim(lower(venue_city)) as venue_city,
		trim(lower(venue_country)) as venue_country
	from {{ref('SILVER_CLEANED_TABLE')}}
) 

select * from clened_venue