with ticket_full_solved_time as (
  select ticket.id as ticket_id,
         ticket.created_at as created_at,
         round(timestamp_diff(created_at, timestamp_trunc(created_at, week), second)/60, 0) as start_time_in_minutes_from_week,
         round(timestamp_diff(max(ticket_field_history.updated), created_at, second)/60, 0) as raw_delta_in_minutes
  from [ZENDESK_SCHEMA].ticket
  join [ZENDESK_SCHEMA].ticket_field_history on ticket.id = ticket_field_history.ticket_id
  where ticket_field_history.value = 'solved'
  group by 1, 2
),
weekly_periods as (
  select ticket_id,
         start_time_in_minutes_from_week,
         raw_delta_in_minutes,
         week_number,
         greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
         least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
  from ticket_full_solved_time, unnest(generate_array(0, floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number
),
intercepted_periods as (
  select ticket_id,
         week_number,
         ticket_week_start_time,
         ticket_week_end_time,
         schedule.start_time_utc as schedule_start_time,
         schedule.end_time_utc as schedule_end_time,
         least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
  from weekly_periods
  join [ZENDESK_SCHEMA].schedule on ticket_week_start_time <= schedule.end_time_utc and ticket_week_end_time >= schedule.start_time_utc
),
business_minutes as (
  select ticket_id,
         sum(scheduled_minutes) as full_resolution_time_in_business_minutes
  from intercepted_periods
  group by 1
  order by 1
)
select business_minutes.ticket_id,
       calendar_minutes.raw_delta_in_minutes as full_resolution_time_in_calendar_minutes,
       business_minutes.full_resolution_time_in_business_minutes
from business_minutes
join ticket_full_solved_time as calendar_minutes
  on business_minutes.ticket_id = calendar_minutes.ticket_id
