with source as (

    select * from {{ source('raw', 'raw_events') }}

),

renamed as (

    select
        study_id,
        case 
            when event_type in ('apnea', 'hypopnea', 'arousal') then event_type
            else 'other'
        end as event_type,
        duration_sec,
        study_datetime,
        to_date(study_datetime) as event_date,
        current_timestamp() as ingestion_time

    from source

)

select * from renamed
