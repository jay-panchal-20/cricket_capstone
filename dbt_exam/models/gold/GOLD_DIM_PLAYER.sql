with cte as(
    select distinct
        batsman_id,
        batsman_name,
        batsman_dob,
        batsman_nationality,
        batting_hand,
        batsman_role,
        batsman_email,
        batsman_phone,
        contract_id,
        batting_team_id,
        row_number() over(partition by batsman_id order by delivery_timestamp desc) as rn
    from cricket.dbo.Silver_cleaned_table
    where batsman_name is not null and
        batsman_dob is not null and
        batsman_nationality is not null and
        batting_hand is not null and
        batsman_role is not null and
        batsman_email is not null and
        batsman_phone is not null


),
cte2 as (
    select distinct
        bowler_id,
        bowler_name,
        bowler_nationality,
        bowling_style,
        row_number() over(partition by bowler_id order by delivery_timestamp desc) as rn
    from cricket.dbo.Silver_cleaned_table
    where bowler_name is not null and
        bowler_nationality is not null and
        bowling_style is not null
),

cte3 as(
    select 
        bowler_id,
        bowler_name,
        bowler_nationality,
        bowling_style
    from cte2 where rn = 1
),

cte4 as(
    select 
        batsman_id,
        batsman_name,
        batsman_dob,
        batsman_nationality,
        batting_hand,
        batsman_role,
        batsman_email,
        batsman_phone,
        batting_team_id,
        contract_id
    from cte where rn = 1
)

select 
	c4.batsman_id as player_id,
	c4.batsman_name as player_name,
	c4.batsman_dob as player_dob,
	c4.batsman_nationality as nationality,
	c4.batting_hand,
	c4.batsman_role as player_role,
	c4.batsman_email email,
	c4.batsman_phone phone,
	c3.bowling_style,
    c4.batting_team_id as team_id,
	c4.contract_id as contract_id

from cte3 c3 right join cte4 c4
on c3.bowler_id = c4.batsman_id
