# Zendesk Ticket Metrics

These queries will re-create the Zendesk ticket metrics using the Fivetran data schema. There are two options:

For Zendesk Enterprise clients, it is recommended to use the `multiple_schedules` VIEWs.  Zendesk's own ticket metric calculations only use the business hours schedule at the time the metric is calculated, which results in incorrect calculations for first response time, first resolution time, full resolution time, and wait times.

For non-Enterprise clients, it is recommended to use the `single_schedules` VIEWs.  These are still an improvement over Zendesk's own ticket metric calculations because it predictably uses the schedule in use prior to when an event happens. Zendesk's own ticket metric calculations have a race-condition when an event (such as changing the status of a ticket to "solved") triggers a schedule changed

## Instructions

Using the provided SQL queries, create the following 4 views

1. reply_time, 
1. first_resolution_time, 
1. full_resolution_time, 
1. wait_times

once these views have been created use ticket_metrics.sql to create the `ticket_metrics` VIEW that combines them into one table.

#### Note: that all queries assume that the schema name is "zendesk", so a rename will be needed if the schema name used is different

[](https://user-images.githubusercontent.com/8846529/45316884-34bf4800-b4ed-11e8-98c8-74eae336cd8f.png)

[](https://user-images.githubusercontent.com/8846529/45316885-34bf4800-b4ed-11e8-9b99-334bfadb5c99.png)

[](https://user-images.githubusercontent.com/8846529/45316886-34bf4800-b4ed-11e8-8abc-5f91a6de5c3c.png)
