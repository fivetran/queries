-- full resolution time in minutes
with
-- obtaining latest solved time
ticket_full_solved_at as (
  select ticket_id,
         max(ticket_field_history.updated) as solved_at
  from zendesk.ticket_field_history
  where ticket_field_history.value = 'solved'
  group by 1
),
ticket_status_timeline as (
  select ticket_id,
         value,
         updated as status_start,
         coalesce(lead(updated, 1) over ticket_status_timeline, current_timestamp()) as status_end
  from zendesk.ticket_field_history
  where field_name = 'status'
  window ticket_status_timeline as (partition by ticket_id order by updated)
),
-- picking latest valid created_at for ticket_schedule combination
ticket_schedule_created_picked as (
  select ticket_schedule.ticket_id,
         max(ticket_schedule.created_at) as latest_valid_schedule_created_at
  from zendesk.ticket_schedule
  join ticket_full_solved_at on ticket_full_solved_at.ticket_id = ticket_schedule.ticket_id
  where ticket_schedule.created_at < ticket_full_solved_at.solved_at
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
-- defining the start_time_in_minutes from a week start, and the raw number of calendar minutes taken until solved
ticket_full_solved_time as (
  select ticket.id as ticket_id,
         ticket_status_timeline.status_start,
         ticket_status_timeline.status_end,
         ticket_schedule_assigned.schedule_id,
         ticket_status_timeline.value as ticket_status,
         round(timestamp_diff(ticket_status_timeline.status_start, timestamp_trunc(ticket_status_timeline.status_start, week), second)/60, 0) as start_time_in_minutes_from_week,
         round(timestamp_diff(ticket_status_timeline.status_end, ticket_status_timeline.status_start, second)/60, 0) as raw_delta_in_minutes
  from zendesk.ticket
  left join ticket_status_timeline on ticket.id = ticket_status_timeline.ticket_id
  left join ticket_schedule_assigned on ticket.id = ticket_schedule_assigned.ticket_id
  group by 1, 2, 3, 4, 5
),
-- breaking the calednar solved time into weekly periods to intercept with weekly business hours
weekly_periods as (
  select ticket_id,
         schedule_id,
         start_time_in_minutes_from_week,
         raw_delta_in_minutes,
         week_number,
         ticket_status,
         greatest(0, start_time_in_minutes_from_week - week_number * (7*24*60)) as ticket_week_start_time,
         least(start_time_in_minutes_from_week + raw_delta_in_minutes - week_number * (7*24*60), (7*24*60)) as ticket_week_end_time
  from ticket_full_solved_time, unnest(generate_array(0, floor((start_time_in_minutes_from_week + raw_delta_in_minutes) / (7*24*60)), 1)) as week_number
),
-- intercepting weeks with business hours to calculate business minutes in each business period
intercepted_periods as (
  select ticket_id,
         week_number,
         schedule_id,
         ticket_week_start_time,
         ticket_week_end_time,
         ticket_status,
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
         ticket_status,
         if(ticket_status in ('pending'), scheduled_minutes, 0) as agent_wait_time_in_minutes,
         if(ticket_status in ('new', 'open', 'hold'), scheduled_minutes, 0) as requester_wait_time_in_minutes,
         if(ticket_status in ('hold'), scheduled_minutes, 0) as on_hold_time_in_minutes
  from intercepted_periods
),
business_minutes_aggregated as (
  select ticket_id,
         sum(agent_wait_time_in_minutes) as agent_wait_time_in_minutes,
         sum(requester_wait_time_in_minutes) as requester_wait_time_in_minutes,
         sum(on_hold_time_in_minutes) as on_hold_time_in_minutes
  from business_minutes
  group by 1
),
calendar_minutes as (
 select ticket_id,
        ticket_status,
        if(ticket_status in ('pending'), raw_delta_in_minutes, 0) as agent_wait_time_in_minutes,
        if(ticket_status in ('new', 'open', 'hold'), raw_delta_in_minutes, 0) as requester_wait_time_in_minutes,
        if(ticket_status in ('hold'), raw_delta_in_minutes, 0) as on_hold_time_in_minutes
 from ticket_full_solved_time
),
calendar_minutes_aggregated as (
  select ticket_id,
         sum(agent_wait_time_in_minutes) as agent_wait_time_in_minutes,
         sum(requester_wait_time_in_minutes) as requester_wait_time_in_minutes,
         sum(on_hold_time_in_minutes) as on_hold_time_in_minutes
  from calendar_minutes
  group by 1
)
--attaching the calendar minutes in addition to business minutes
select calendar_minutes_aggregated.ticket_id,
       calendar_minutes_aggregated.agent_wait_time_in_minutes as agent_wait_time_in_calendar_minutes,
       business_minutes_aggregated.agent_wait_time_in_minutes as agent_wait_time_in_business_minutes,
       calendar_minutes_aggregated.requester_wait_time_in_minutes as requester_wait_time_in_calendar_minutes,
       business_minutes_aggregated.requester_wait_time_in_minutes as requester_wait_time_in_business_minutes,
       calendar_minutes_aggregated.on_hold_time_in_minutes as on_hold_time_in_calendar_minutes,
       business_minutes_aggregated.on_hold_time_in_minutes as on_hold_time_in_business_minutes
from calendar_minutes_aggregated
left join business_minutes_aggregated
  on business_minutes_aggregated.ticket_id = calendar_minutes_aggregated.ticket_id
