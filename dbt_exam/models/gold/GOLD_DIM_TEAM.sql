{{
    config(
        materialized='table'
    )
}}


with cte as(
	select 
		batting_team_id as team_id,
		batting_team_name as team_name
	from cricket.dbo.Silver_cleaned_table

	union

	select 
		bowling_team_id as team_id,
		bowling_team_name as team_name
	from {{ref('SILVER_CLEANED_TABLE')}}
)

select * from cte