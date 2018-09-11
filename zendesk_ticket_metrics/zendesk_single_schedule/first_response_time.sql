-- first response time in minutes
with
-- obtaining latest responded time
ticket_first_responded_at as (
  select ticket_comment.ticket_id,
         min(created) as responded_at
  from zendesk.ticket_comment
  join zendesk.user
    on ticket_comment.user_id = user.id
  where user.role in ('admin', 'agent') and public
  group by ticket_id
),
-- picking latest valid created_at for ticket_schedule combination
ticket_schedule_created_picked as (
  select ticket_schedule.ticket_id,
         max(ticket_schedule.created_at) as latest_valid_schedule_created_at
  from zendesk.ticket_schedule
  join ticket_first_responded_at on ticket_first_responded_at.ticket_id = ticket_schedule.ticket_id
  where ticket_schedule.created_at < ticket_first_responded_at.responded_at
  group by 1
),
-- obtaining the schedule_id that will be used for calculating business hours
ticket_schedule_assigned as (
  select ticket_schedule_created_picked.ticket_id,
         ticket_schedule.schedule_id
  from ticket_schedule_created_picked
  join zendesk.ticket_schedule on ticket_schedule_created_picked.ticket_id = ticket_schedule.ticket_id
    and ticket_schedule.created_at = ticket_schedule_created_picked.latest_valid_schedule_created_at
),
-- defining the start_time_in_minutes from a week start, and the raw number of calendar minutes taken until responded
ticket_first_responded_time as (
  select ticket.id as ticket_id,
         ticket.created_at,
         ticket_schedule_assigned.schedule_id,
         round(timestamp_diff(ticket.created_at, timestamp_trunc(ticket.created_at, week), second)/60, 0) as start_time_in_minutes_from_week,
         greatest(0, round(timestamp_diff(max(ticket_first_responded_at.responded_at), ticket.created_at, second)/60, 0)) as raw_delta_in_minutes
  from zendesk.ticket
  left join ticket_first_responded_at on ticket.id = ticket_first_responded_at.ticket_id
  left join ticket_schedule_assigned on ticket.id = ticket_schedule_assigned.ticket_id
  group by 1, 2, 3
),
-- breaking the calednar responded time into weekly periods to intercept with weekly business hours
weekly_periods as (
  select ticket_id,
         start_time_in_minutes_from_week,
         raw_delta_in_minutes,
         week_number,
         schedule_id,
         greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
         least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
  from ticket_first_responded_time, unnest(generate_array(0, floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number
),
-- intercepting weeks with business hours to calculate business minutes in each business period
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
-- summing up business minutes for each ticket
business_minutes as (
  select ticket_id,
         sum(scheduled_minutes) as first_response_time_in_business_minutes
  from intercepted_periods
  group by 1
  order by 1
)
--attaching the calendar minutes in addition to business minutes
select ticket_first_responded_time.ticket_id,
       business_minutes.first_response_time_in_business_minutes,
       sum(ticket_first_responded_time.raw_delta_in_minutes) as first_response_time_in_calendar_minutes
from ticket_first_responded_time
left join business_minutes
  on business_minutes.ticket_id = ticket_first_responded_time.ticket_id
group by 1, 2
