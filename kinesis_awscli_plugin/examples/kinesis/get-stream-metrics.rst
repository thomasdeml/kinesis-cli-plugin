** get-stream-metrics **

The following examples display stream-level metrics for the specified stream. By default datapoints of the last 10 minutes get fetched.

** Example 1: **

Fetches all metrics for the specified stream:

aws kinesis get-stream-metrics --stream-name Test

** Example 2: **

Fetches all metrics for the specified stream and displays them in readable format:

aws kinesis get-stream-metrics --stream-name Test --output table
