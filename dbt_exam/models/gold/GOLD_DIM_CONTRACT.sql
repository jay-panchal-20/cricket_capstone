{{
    config(
        materialized='table'
    )
}}

with cte as(
select
	batsman_id,
	contract_id,
	contract_grade,
	annual_retainer_usd,
	contract_status,
	row_number() over (partition by contract_id order by batsman_id) as rn
from cricket.dbo.Silver_cleaned_table
where contract_id is not null
)
select 
	batsman_id,
	contract_id,
	contract_grade,
	annual_retainer_usd,
	contract_status
from cte where rn = 1
