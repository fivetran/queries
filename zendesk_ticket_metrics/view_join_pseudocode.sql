CREATE VIEW zendesk.ticket_metrics AS (
WITH group_stations AS (
    SELECT
        ticket_id,
        COUNT(distinct value) AS number
    FROM zendesk.ticket_field_history
    WHERE field_name = 'group_id' 
    GROUP BY ticket_id
),
assignee_stations AS (
    SELECT
        ticket_id,
        COUNT(distinct value) AS number
    FROM zendesk.ticket_field_history
    WHERE field_name = 'assignee_id' 
    GROUP BY ticket_id
),
grouped_ticket_status_history AS ( 
    SELECT * 
    FROM zendesk.ticket_field_history
    WHERE field_name = 'status' 
    ORDER BY ticket_id, updated
),
ticket_status_changes AS ( 
    SELECT 
        ticket_id,
        LAG(ticket_id, 1, 0) OVER(ORDER BY ticket_id, updated) AS prev_ticket_id, 
        value AS status, 
        LAG(value, 1, 'new') OVER(ORDER BY ticket_id, updated) AS prev_status
    FROM grouped_ticket_status_history
),
reopens AS (
    SELECT 
        DISTINCT ticket_id, 
        COUNT(ticket_id) AS reopens
    FROM ticket_status_changes 
    WHERE ticket_id = prev_ticket_id AND prev_status = 'solved' AND status = 'open' 
    GROUP BY ticket_id
),
replies AS (
    SELECT
        DISTINCT ticket_id,
        COUNT(ticket_id) AS replies
    FROM zendesk.ticket_comment
    WHERE public
    GROUP BY ticket_id
),
assignee_updated_at AS (
    SELECT
        ticket_id,
        MAX(updated) AS assignee_updated_at
    FROM zendesk.ticket_field_history
    WHERE field_name = 'assignee_id' 
    GROUP BY ticket_id

),
requester_updated_at AS (
    SELECT
        ticket_id,
        MAX(updated) AS requester_updated_at
    FROM zendesk.ticket_field_history
    WHERE field_name = 'requester_id' 
    GROUP BY ticket_id
),
status_updated_at AS (
    SELECT
        ticket_id,
        MAX(updated) AS status_updated_at
    FROM zendesk.ticket_field_history
    WHERE field_name = 'status' 
    GROUP BY ticket_id
),
initially_assigned_at AS (
    SELECT
        ticket_id,
        MIN(updated) AS initially_assigned_at
    FROM zendesk.ticket_field_history
    WHERE field_name = 'assignee_id' 
    GROUP BY ticket_id
),
assigned_at AS (
    SELECT
        ticket_id,
        MAX(updated) AS assigned_at
    FROM zendesk.ticket_field_history
    WHERE field_name = 'assignee_id' 
    GROUP BY ticket_id
),
solved_at AS (
    SELECT
        ticket_id,
        MAX(updated) AS solved_at
    FROM zendesk.ticket_field_history
    WHERE value = 'solved' 
    GROUP BY ticket_id
),
latest_comment_added_at AS (
    SELECT
        ticket_id,
        MAX(created) AS latest_comment_added_at
    FROM zendesk.ticket_comment 
    GROUP BY ticket_id
)
SELECT ticket.id AS ticket_id,
    group_stations.group_stations,
    assignee_stations.assignee_stations,
    reopens.reopens,
    replies.replies,
    assignee_updated_at.assignee_updated_at,
    requester_updated_at.requester_updated_at,
    status_updated_at.status_updated_at,
    initially_assigned_at.initially_assigned_at,
    assigned_at.assigned_at,
    solved_at.solved_at,
    latest_comment_added_at.latest_comment_added_at,
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
FROM zendesk.ticket
LEFT JOIN group_stations ON ticket.id = group_stations.ticket_id
LEFT JOIN assignee_stations oONn ticket.id = assignee_stations.ticket_id
LEFT JOIN reopens ON ticket.id = reopens.ticket_id
LEFT JOIN replies ON ticket.id = replies.ticket_id
LEFT JOIN assignee_updated_at ON ticket.id = assignee_updated_at.ticket_id
LEFT JOIN requester_updated_at ON ticket.id = requester_updated_at.ticket_id
LEFT JOIN status_updated_at ON ticket.id = status_updated_at.ticket_id
LEFT JOIN initially_assigned_at ON ticket.id = initially_assigned_at.ticket_id
LEFT JOIN assigned_at ON ticket.id = assigned_at.ticket_id
LEFT JOIN solved_at ON ticket.id = solved_at.ticket_id
LEFT JOIN latest_comment_added_at ON ticket.id = latest_comment_added_at.id
LEFT JOIN first_response_time ON ticket.id = first_response_time.ticket_id
LEFT JOIN first_resolution_time ON ticket.id = first_resolution_time.ticket_id
LEFT JOIN full_resolution_time ON ticket.id = full_resolution_time.ticket_id
LEFT JOIN wait_times ON ticket.id = wait_times.ticket_id 
);
