with studies as (

    select * from {{ ref('stg_sleep_study') }}

)

select
    study_id,
    study_date,
    total_sleep_time_min,
    rem_min,
    deep_min,
    light_min,
    age_group,
    sex,
    (rem_min / nullif(total_sleep_time_min, 0)) * 100 as rem_percentage

from studies
