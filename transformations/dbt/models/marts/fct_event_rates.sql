with events as (
    select * from {{ ref('stg_events') }}
),

aggregated as (
    select
        study_id,
        event_date,
        count(*) as total_events,
        sum(case when event_type = 'apnea' then 1 else 0 end)
            as apnea_count,
        sum(case when event_type = 'hypopnea' then 1 else 0 end)
            as hypopnea_count,
        sum(case when event_type = 'arousal' then 1 else 0 end)
            as arousal_count,
        avg(duration_sec) as avg_event_duration_sec
    from events
    group by 1, 2
)

select
    a.study_id,
    a.event_date,
    a.total_events,
    a.apnea_count,
    a.hypopnea_count,
    a.arousal_count,
    a.avg_event_duration_sec
from aggregated as a
