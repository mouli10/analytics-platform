with source as (
    select
        EVENT_ID as event_id,
        USER_ID as user_id,
        EVENT_TYPE as event_type,
        EVENT_TIMESTAMP as event_timestamp
    from {{ source('raw', 'app_events_raw') }}
)

select * from source
