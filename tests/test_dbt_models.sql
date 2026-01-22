-- Test that total duration is greater than sum of stages (logical check)
select *
from {{ ref('fct_sleep_summary') }}
where total_sleep_time_min < (rem_min + deep_min + light_min - 1) -- allow small rounding buffer
