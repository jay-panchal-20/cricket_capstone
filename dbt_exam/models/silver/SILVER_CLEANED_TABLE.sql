{{
    config(
        materialized='table'
    )
}}


with cte as(
select 
	try_cast(trim(delivery_id) as varchar(50)) as delivery_id,
	coalesce(try_cast(trim(delivery_timestamp) as datetime),
				try_cast(trim(concat(SUBSTRING(delivery_timestamp,7,4),'-',SUBSTRING(delivery_timestamp,4,2),'-',SUBSTRING(delivery_timestamp,1,2),' ',SUBSTRING(delivery_timestamp,12,6))) as datetime)
			) as delivery_timestamp,
	try_cast(trim(innings_no) as int) as innings_no,
	TRY_CAST(trim(over_no) as int) as over_no,
	TRY_CAST(trim(ball_in_over) as int) as ball_in_over,
	try_cast(trim(runs_off_bat) as int) as runs_off_bat,
	try_cast(trim(lower(extra_type)) as varchar(50)) as extra_type,
	try_cast(trim(extra_runs) as int) as extra_runs,
	try_cast(trim(total_runs) as int) as total_runs,
	try_cast(trim(is_wicket) as bit) as is_wicket,

	try_cast(lower(trim(dismissal_type)) as varchar(50)) as dismissal_type,
	try_cast(trim(ball_speed_kmph) as numeric(10,2)) as ball_speed_kmph,
	try_cast(trim(non_striker_id) as varchar(50)) as non_striker_id,
	try_cast(trim(lower(shot_type)) as varchar(50)) as shot_type,
	try_cast(trim(lower(shot_region)) as varchar(50)) as shot_region,
	try_cast(trim(match_id) as varchar(50)) as match_id,
	try_cast(trim(match_date) as date) as match_date,
	try_cast(trim(lower(match_format)) as varchar(20)) as match_format,
	try_cast(trim(lower(tournament_name)) as varchar(100)) as tournament_name,
	try_cast(trim(season) as int) as season,
	try_cast(trim(lower(match_stage)) as varchar(50)) as match_stage,
	try_cast(trim(lower(toss_decision)) as varchar(50)) as toss_decision,
	try_cast(trim(venue_id) as varchar(50)) as venue_id,
	try_cast(trim(lower(venue_name)) as varchar(50)) as venue_name,
	try_cast(trim(lower(venue_city)) as varchar(50)) as venue_city,
	try_cast(trim(lower(venue_country)) as varchar(50)) as venue_country,
	try_cast(trim(batting_team_id) as varchar(50)) as batting_team_id,
	try_cast(trim(lower(batting_team_name)) as varchar(50)) as batting_team_name,
	try_cast(trim(bowling_team_id) as varchar(50)) as bowling_team_id,
	try_cast(trim(lower(bowling_team_name)) as varchar(50)) as bowling_team_name,
	try_cast(trim(batsman_id) as varchar(50)) as batsman_id,
	try_cast(trim(lower(batsman_name)) as varchar(50)) as batsman_name,
	try_cast(trim(batsman_dob) as date) as batsman_dob,
	try_cast(trim(lower(batsman_nationality)) as varchar(50)) as batsman_nationality,
	try_cast(trim(lower(batting_hand)) as varchar(50)) as batting_hand,
	try_cast(trim(lower(batsman_role)) as varchar(50)) as batsman_role,

    case when batsman_email like '%@%.%' then
	    try_cast(trim(batsman_email) as  varchar(100))
        else null
    end as batsman_email,

    case 
        when batsman_phone = 'N/A' 
            then null
        else 
	        try_cast(trim(batsman_phone) as varchar(50)) 
    end as batsman_phone,

	try_cast(trim(batsman_icc_rating) as int) as batsman_icc_rating,

	try_cast(trim(bowler_id) as varchar(50)) as bowler_id,
	try_cast(trim(lower(bowler_name)) as varchar(50)) as bowler_name,
	try_cast(trim(lower(bowler_nationality)) as varchar(50)) as bowler_nationality,
	try_cast(trim(lower(bowling_style)) as varchar(50)) as bowling_style,
	try_cast(trim(bowler_icc_rating) as int) as bowler_icc_rating,

	try_cast(trim(contract_id) as varchar(50)) as contract_id,
	try_cast(trim(lower(contract_grade)) as varchar(50)) as contract_grade,
	try_cast(trim(annual_retainer_usd) as float) as annual_retainer_usd,
	try_cast(trim(lower(contract_status)) as varchar(50)) as contract_status,

	try_cast(trim(drs_review_id) as varchar(50)) as drs_review_id,
	try_cast(trim(lower(drs_reviewed_by)) as varchar(50)) as drs_reviewed_by,
	try_cast(trim(lower(drs_original_decision)) as varchar(50)) as drs_original_decision,
	try_cast(trim(lower(drs_outcome)) as varchar(50)) as drs_outcome,
	CURRENT_TIMESTAMP as silver_ingest_date
 
from {{source('BRONZE','BRONZE_RAW_TABLE')}}

),

cte2 as(

select *,row_number() over(partition by delivery_id order by delivery_timestamp desc) as rn
from cte
)

select 
                    delivery_id,
                    delivery_timestamp ,
                    innings_no ,
                    over_no ,
                    ball_in_over ,
                    runs_off_bat ,
                    extra_type ,
                    extra_runs ,
                    total_runs ,
                    is_wicket ,
                    dismissal_type ,
                    ball_speed_kmph ,
                    non_striker_id ,
                    shot_type ,
                    shot_region ,
                    match_id ,
                    match_date ,
                    match_format ,
                    tournament_name ,
                    season ,
                    match_stage ,
                    toss_decision ,
                    venue_id ,
                    venue_name ,
                    venue_city ,
                    venue_country ,
                    batting_team_id ,
                    batting_team_name ,
                    bowling_team_id ,
                    bowling_team_name ,
                    batsman_id ,
                    batsman_name ,
                    batsman_dob ,
                    batsman_nationality ,
                    batting_hand ,
                    batsman_role ,
                    batsman_email ,
                    batsman_phone ,
                    batsman_icc_rating ,
                    bowler_id ,
                    bowler_name ,
                    bowler_nationality ,
                    bowling_style ,
                    bowler_icc_rating ,
                    contract_id ,
                    contract_grade ,
                    annual_retainer_usd ,
                    contract_status ,
                    drs_review_id ,
                    drs_reviewed_by ,
                    drs_original_decision ,
                    drs_outcome ,
                    silver_ingest_date
from cte2 where rn = 1