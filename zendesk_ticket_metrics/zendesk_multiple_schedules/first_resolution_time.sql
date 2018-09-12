CREATE OR REPLACE VIEW zendesk.first_resolution_time AS (
with ticket_schedule as (
  select ticket_id,
         schedule_id,
         created_at as schedule_created_at,
         coalesce(lead(created_at, 1) over ticket_schedule_timeline, timestamp("9999-12-31 01:01:01")) as schedule_invalidated_at
  from zendesk.ticket_schedule
  window ticket_schedule_timeline as (partition by ticket_id order by created_at)
),
ticket_first_solved_time as (
  select ticket.id as ticket_id,
         ticket_schedule.schedule_created_at,
         ticket_schedule.schedule_invalidated_at,
         ticket_schedule.schedule_id,
         round(timestamp_diff(ticket_schedule.schedule_created_at, timestamp_trunc(ticket_schedule.schedule_created_at, week), second)/60, 0) as start_time_in_minutes_from_week,
         greatest(0, round(timestamp_diff(least(ticket_schedule.schedule_invalidated_at, min(ticket_field_history.updated)), ticket_schedule.schedule_created_at, second)/60, 0)) as raw_delta_in_minutes
  from zendesk.ticket
  left join ticket_schedule on ticket.id = ticket_schedule.ticket_id
  left join zendesk.ticket_field_history on ticket.id = ticket_field_history.ticket_id
  where ticket_field_history.value = 'solved'
  group by 1, 2, 3, 4
),
weekly_periods as (
  select ticket_id,
         start_time_in_minutes_from_week,
         raw_delta_in_minutes,
         week_number,
         schedule_id,
         greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
         least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
  from ticket_first_solved_time, unnest(generate_array(0, floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number
),
intercepted_periods as (
  select ticket_id,
         week_number,
         schedule_id,
         ticket_week_start_time,
         ticket_week_end_time,
         schedule.start_time_utc as schedule_start_time,
         schedule.end_time_utc as schedule_end_time,
         least(ticket_week_end_time, schedule.end_time_utc) - greatest(ticket_week_start_time, schedule.start_time_utc) as scheduled_minutes
  from weekly_periods
  join zendesk.schedule on ticket_week_start_time <= schedule.end_time_utc and ticket_week_end_time >= schedule.start_time_utc
    and weekly_periods.schedule_id = schedule.id
),
business_minutes as (
  select ticket_id,
         sum(scheduled_minutes) as first_resolution_time_in_business_minutes
  from intercepted_periods
  group by 1
  order by 1
),
calendar_minutes as (
  select ticket_id,
         sum(raw_delta_in_minutes) as first_resolution_time_in_calendar_minutes
  from ticket_first_solved_time
  group by 1
)
select calendar_minutes.ticket_id,
       calendar_minutes.first_resolution_time_in_calendar_minutes,
       business_minutes.first_resolution_time_in_business_minutes
from calendar_minutes
left join business_minutes
  on business_minutes.ticket_id = calendar_minutes.ticket_id
);
