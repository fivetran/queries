create or replace view zendesk.wait_times as (
with ticket_status_timeline as (
    select
        ticket_id
        , value
        , updated as status_start
        , coalesce(lead(updated, 1) over (partition by ticket_id order by updated), current_timestamp()) as status_end
    from zendesk.ticket_field_history
    where field_name = 'status'
),
ticket_schedule as (
    select
        ticket.id as ticket_id
        , ticket_schedule.schedule_id
        , coalesce(ticket_schedule.created_at, ticket.created_at) as schedule_created_at
        , coalesce(lead(schedule_created_at, 1) over (partition by ticket.id order by schedule_created_at), '9999-12-31 01:01:01'::timestamp) as schedule_invalidated_at
    from zendesk.ticket
    inner join zendesk.brand
        on ticket.brand_id = brand.id
    left join zendesk.ticket_schedule
        on ticket.id = ticket_schedule.ticket_id
),
ticket_status_crossed_with_schedule as (
    select
        ticket_status_timeline.ticket_id
        , ticket_status_timeline.value
        , ticket_schedule.schedule_id
        , greatest(status_start, schedule_created_at) as status_schedule_start
        , least(status_end, schedule_invalidated_at) as status_schedule_end
    from ticket_status_timeline
    left join ticket_schedule
        on ticket_status_timeline.ticket_id = ticket_schedule.ticket_id
    where timestampdiff(second, greatest(status_start, schedule_created_at), least(status_end, schedule_invalidated_at)) > 0
),
ticket_full_solved_time as (
    select 
        ticket_status_crossed_with_schedule.ticket_id
        , ticket_status_crossed_with_schedule.status_schedule_start
        , ticket_status_crossed_with_schedule.status_schedule_end
        , ticket_status_crossed_with_schedule.schedule_id
        , ticket_status_crossed_with_schedule.value as ticket_status
        , round(timestampdiff(second, date_trunc(week, ticket_status_crossed_with_schedule.status_schedule_start), ticket_status_crossed_with_schedule.status_schedule_start)/60, 0) as start_time_in_minutes_from_week
        , round(timestampdiff(second, ticket_status_crossed_with_schedule.status_schedule_start, ticket_status_crossed_with_schedule.status_schedule_end)/60, 0) as raw_delta_in_minutes
    from ticket_status_crossed_with_schedule
    left join ticket_schedule on ticket_schedule.ticket_id = ticket_status_crossed_with_schedule.ticket_id
    group by 1, 2, 3, 4, 5
),
weekly_periods as (
    select
        ticket_id
        , start_time_in_minutes_from_week
        , raw_delta_in_minutes
        , row_number() over (partition by ticket_id, ticket_status, status_schedule_start order by 1, 2, 3) - 1 as week_number
        , schedule_id
        , ticket_status
        , greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time
        , least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
    from ticket_full_solved_time
    /*
    fivetran's original query used a `generate_array` function which
    is unavailable here. we accomplish something similar by cross joining
    a series generator with the `row_number` window function and filtering
    the result. week_number is zero-indexed.
    */
    cross join table(generator(rowcount => 52 * 3)) -- can't generate arbitrary lengths, increase this number if 3 years is not enough time
    qualify row_number() over (partition by ticket_id, ticket_status, status_schedule_start order by 1, 2, 3) - 1 <= floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60))
),
intercepted_periods as (
    select 
        ticket_id
        , week_number
        , schedule_id
        , ticket_status
        , ticket_week_start_time
        , ticket_week_end_time
        , schedule.start_time_utc as schedule_start_time
        , schedule.end_time_utc as schedule_end_time
        , least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
    from weekly_periods
    left join zendesk.schedule
        on ticket_week_start_time <= schedule.end_time_utc
        and ticket_week_end_time >= schedule.start_time_utc
        and weekly_periods.schedule_id = schedule.id
),
business_minutes as (
    select
        ticket_id
        , ticket_status
        , iff(ticket_status in ('pending'), scheduled_minutes, 0) as agent_wait_time_in_minutes
        , iff(ticket_status in ('new', 'open', 'hold'), scheduled_minutes, 0) as requester_wait_time_in_minutes
        , iff(ticket_status in ('hold'), scheduled_minutes, 0) as on_hold_time_in_minutes
    from intercepted_periods
),
business_minutes_aggregated as (
    select 
        ticket_id
        , sum(agent_wait_time_in_minutes) as agent_wait_time_in_minutes
        , sum(requester_wait_time_in_minutes) as requester_wait_time_in_minutes
        , sum(on_hold_time_in_minutes) as on_hold_time_in_minutes
    from business_minutes
    group by 1
),
calendar_minutes as (
    select 
        ticket_id
        , ticket_status
        , iff(ticket_status in ('pending'), raw_delta_in_minutes, 0) as agent_wait_time_in_minutes
        , iff(ticket_status in ('new', 'open', 'hold'), raw_delta_in_minutes, 0) as requester_wait_time_in_minutes
        , iff(ticket_status in ('hold'), raw_delta_in_minutes, 0) as on_hold_time_in_minutes
    from ticket_full_solved_time
),
calendar_minutes_aggregated as (
    select
        ticket_id
        , sum(agent_wait_time_in_minutes) as agent_wait_time_in_minutes
        , sum(requester_wait_time_in_minutes) as requester_wait_time_in_minutes
        , sum(on_hold_time_in_minutes) as on_hold_time_in_minutes
    from calendar_minutes
    group by 1
)
select 
    calendar_minutes_aggregated.ticket_id
    , calendar_minutes_aggregated.agent_wait_time_in_minutes as agent_wait_time_in_calendar_minutes
    , business_minutes_aggregated.agent_wait_time_in_minutes as agent_wait_time_in_business_minutes
    , calendar_minutes_aggregated.requester_wait_time_in_minutes as requester_wait_time_in_calendar_minutes
    , business_minutes_aggregated.requester_wait_time_in_minutes as requester_wait_time_in_business_minutes
    , calendar_minutes_aggregated.on_hold_time_in_minutes as on_hold_time_in_calendar_minutes
    , business_minutes_aggregated.on_hold_time_in_minutes as on_hold_time_in_business_minutes
from calendar_minutes_aggregated
left join business_minutes_aggregated
    on business_minutes_aggregated.ticket_id = calendar_minutes_aggregated.ticket_id
);
