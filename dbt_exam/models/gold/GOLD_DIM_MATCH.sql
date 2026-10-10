{{
    config(
        materialized='table'
    )
}}

with cte as(
select 
	match_id,
	match_date,
	match_format,
	tournament_name,
	season,
	match_stage,
	toss_decision,
	venue_id,
	row_number() over(partition by match_id order by match_date desc) as rn
from cricket.dbo.Silver_cleaned_table 
)

select 
	match_id,
	match_date,
	match_format,
	tournament_name,
	season,
	match_stage,
	toss_decision,
	venue_id
from cte where rn = 1