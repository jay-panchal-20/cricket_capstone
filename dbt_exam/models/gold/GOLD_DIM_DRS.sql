{{
    config(
        materialized='table'
    )
}}

select distinct
	drs_review_id,
	drs_reviewed_by,
	drs_original_decision,
	drs_outcome
from {{ref('SILVER_CLEANED_TABLE')}}
where drs_review_id is not null