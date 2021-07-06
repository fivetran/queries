--- Create the 4 dependent views before executing this
create or replace view zendesk.ticket_metrics as (
WITH group_stations AS (
    SELECT
        ticket_id,
        COUNT(distinct value) AS group_stations
    FROM zendesk.ticket_field_history
    WHERE field_name = 'group_id' 
    GROUP BY ticket_id
),
assignee_stations AS (
    SELECT
        ticket_id,
        COUNT(distinct value) AS assignee_stations
    FROM zendesk.ticket_field_history
    WHERE field_name = 'assignee_id' 
    GROUP BY ticket_id
),
reopens AS (
    SELECT 
        ticket_id, 
        COUNT(ticket_id) AS reopens
    FROM ( 
        SELECT 
            ticket_id,
            value AS status, 
            LAG(value, 1) OVER (PARTITION BY ticket_id ORDER BY updated) AS prev_status
        FROM zendesk.ticket_field_history
        WHERE field_name = 'status' 
    ) 
    WHERE prev_status = 'solved' AND status = 'open' 
    GROUP BY ticket_id
),
replies AS (
    SELECT
        ticket_id,
        COUNT(ticket_id) AS replies
    FROM zendesk.ticket_comment
    JOIN zendesk.user
        ON ticket_comment.user_id = user.id
    WHERE
        public
        and user.role in ('admin', 'agent')
    GROUP BY ticket_id
),
assignee_updated_at AS (
    SELECT
        ticket.id as ticket_id,
        max(updated) as assignee_updated_at
    FROM zendesk.ticket
    JOIN zendesk.ticket_field_history 
        ON ticket_field_history.ticket_id = ticket.id AND ticket_field_history.user_id = ticket.assignee_id
    GROUP BY 1
),
requester_updated_at AS (
    SELECT
        ticket.id as ticket_id,
        max(updated) as requester_updated_at
    FROM zendesk.ticket
    JOIN zendesk.ticket_field_history 
        ON ticket_field_history.ticket_id = ticket.id AND ticket_field_history.user_id = ticket.requester_id
    GROUP BY 1
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
SELECT 
    ticket.id AS ticket_id
    , group_stations.group_stations
    , assignee_stations.assignee_stations
    , coalesce(reopens.reopens, 0) as reopens
    , coalesce(replies.replies, 0) as replies
    , assignee_updated_at.assignee_updated_at
    , requester_updated_at.requester_updated_at
    , status_updated_at.status_updated_at
    , initially_assigned_at.initially_assigned_at
    , assigned_at.assigned_at
    , solved_at.solved_at
    , latest_comment_added_at.latest_comment_added_at
    , reply_time.reply_time_in_calendar_minutes
    , reply_time.reply_time_in_business_minutes
    , first_resolution_time.first_resolution_time_in_calendar_minutes
    , first_resolution_time.first_resolution_time_in_business_minutes
    , full_resolution_time.full_resolution_time_in_calendar_minutes
    , full_resolution_time.full_resolution_time_in_business_minutes
    , wait_times.agent_wait_time_in_calendar_minutes
    , wait_times.agent_wait_time_in_business_minutes
    , wait_times.requester_wait_time_in_calendar_minutes
    , wait_times.requester_wait_time_in_business_minutes
    , wait_times.on_hold_time_in_calendar_minutes
    , wait_times.on_hold_time_in_business_minutes
FROM zendesk.ticket
LEFT JOIN group_stations ON ticket.id = group_stations.ticket_id
LEFT JOIN assignee_stations ON ticket.id = assignee_stations.ticket_id
LEFT JOIN reopens ON ticket.id = reopens.ticket_id
LEFT JOIN replies ON ticket.id = replies.ticket_id
LEFT JOIN assignee_updated_at ON ticket.id = assignee_updated_at.ticket_id
LEFT JOIN requester_updated_at ON ticket.id = requester_updated_at.ticket_id
LEFT JOIN status_updated_at ON ticket.id = status_updated_at.ticket_id
LEFT JOIN initially_assigned_at ON ticket.id = initially_assigned_at.ticket_id
LEFT JOIN assigned_at ON ticket.id = assigned_at.ticket_id
LEFT JOIN solved_at ON ticket.id = solved_at.ticket_id
LEFT JOIN latest_comment_added_at ON ticket.id = latest_comment_added_at.ticket_id
LEFT JOIN zendesk.reply_time ON ticket.id = reply_time.ticket_id
LEFT JOIN zendesk.first_resolution_time ON ticket.id = first_resolution_time.ticket_id
LEFT JOIN zendesk.full_resolution_time ON ticket.id = full_resolution_time.ticket_id
LEFT JOIN zendesk.wait_times ON ticket.id = wait_times.ticket_id
);
