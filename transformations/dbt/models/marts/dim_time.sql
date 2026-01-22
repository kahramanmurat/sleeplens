{{ config(materialized='table') }}

with date_spine as (
    select dateadd(day, seq4(), '2020-01-01'::date) as date_day
    from table(generator(rowcount => 3660)) -- ~10 years
)

select
    date_day as date_actual,
    year(date_day) as year,
    month(date_day) as month,
    quarter(date_day) as quarter
from date_spine
