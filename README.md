# xid-for-time

Find the XID of Postgres around a specific time, by querying tables. We use
tables that have `id` and `created_at` columns, where the `id` column is textual
and backed by a monotonic sequence.

The strategy is to use Postgres histogram bounds on the `id` column to quickly
locate a row with a `created_at` close to the target time, then use that row to
find the first row that came before the target.

```console
$ xid-for-time payment_actions '2020-07-19 23:30'
ts=2020-07-22T18:14:15.581429Z event=connect dbname=development host=localhost port=5432 user=postgres
ts=2020-07-22T18:14:16.519614Z event=found_thresholds min_id=PA018W0MT0RG0H min_created_at=2020-07-17T10:09:25.419762Z max_id=PA018YN5RSHS1H max_created_at=2020-07-20T15:17:55.270082Z
ts=2020-07-22T18:14:19.343956Z event=first_past_threshold exceeded_id=PA018X04BZYYQ1 exceeded_created_at=2020-07-17T23:30:01.841065Z exceeded_by=1.841065s
ts=2020-07-22T18:14:19.426905Z event=first_before_threshold before_id=PA018X04BY4YNN before_created_at=2020-07-17T23:29:55.131994Z before_xmin=3673366649 before_by=4.868006s
```
