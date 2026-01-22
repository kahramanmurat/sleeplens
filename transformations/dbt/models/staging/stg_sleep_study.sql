with source as (

    select * from {{ source('raw', 'raw_sleep_studies') }}

),

renamed as (

    select
        study_id,
        study_date,
        total_sleep_time_min,
        rem_min,
        deep_min,
        light_min,
        age_group,
        sex,
        current_timestamp() as ingestion_time

    from source

)

select * from renamed
