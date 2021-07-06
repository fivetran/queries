## Fivetran/Zendesk: Snowflake Queries

### Introduction
The queries in this directory are based on the original queries of the same names in the above directories. These queries have been re-written for compatibility with Snowflake SQL syntax.

### Important Notes
There are a couple of limitations and caveats which you should keep in mind:

* Zendesk considers Sunday to be the first day of the week, but by default Snowflake considers Monday to be the first day of the week. The `date_trunc` function, when called with the `week` argument, will therefore return a result which is incompatible with Zendesk schedule intervals. To get around this, you must set Snowflake's [WEEK_START](https://docs.snowflake.com/en/sql-reference/parameters.html#label-week-start) parameter equal to `7` (Sunday). Failure to do this will result in inaccurate calculations for certain tickets.
* The `weekly_periods` CTE is hard-coded to allow for a maximum of 3 years (`52 * 3`). This is because arguments to Snowflake's `generator` function must be constant. If you need to report on tickets where the resolution period may approach or exceed 3 years, you will need to increase this number.
* Tickets closed by merge (tagged with `closed_by_merge`) are missing values such as SOLVED_AT and values derived from SOLVED_AT, such as resolution time.
* REQUESTER_UPDATED_AT is missing for "voice" tickets.

### Snowflake SQL

There are several functional or syntactic differences between BigQuery SQL and Snowflake SQL that are relevant to these queries. The following is a non-exhaustive list:
- By default, Snowflake considers Monday to be the first day of the week. This is in contrast to BigQuery, which considers Sunday to be the first day. Zendesk also considers the first day of the week to be Sunday. Snowflake's behavior is configurable - see [here](https://docs.snowflake.com/en/sql-reference/functions-date-time.html#first-day-of-the-week).
- Snowflake does not have named window partitions
- Snowflake has no function to generate sequence arrays of arbitrary length
- `timestamp_diff` is called `timestampdiff` in Snowflake SQL
- Snowflake's `timestampdiff` accepts arguments in a different order: the date/time part comes first, and the timestamps themselves are reversed.
- Snowflake uses `IFF` instead of `IF` 