select ticket.id as ticket_id,
       first_response_time.first_resoponse_time_in_calendar_minutes,
       first_response_time.first_resoponse_time_in_business_minutes,
       first_resolution.first_resolution_time_in_calendar_minutes,
       first_resolution.first_resolution_time_in_business_minutes,
       full_resolution.full_resolution_time_in_calendar_minutes,
       full_resolution.full_resolution_time_in_business_minutes,
       wait_times.agent_wait_time_in_calendar_minutes,
       wait_times.agent_wait_time_in_business_minutes,
       wait_times.requester_wait_time_in_calendar_minutes,
       wait_times.requester_wait_time_in_business_minutes,
       wait_times.on_hold_time_in_calendar_minutes,
       wait_times.on_hold_time_in_business_minutes
from zendesk.ticket
left join first_response_time on ticket.id = first_response_time.ticket_id
left join first_resolution_time on ticket.id = first_resolution_time.ticket_id
left join full_resolution_time on ticket.id = full_resolution_time.ticket_id
left join wait_times on ticket.id = wait_times.ticket_id 
