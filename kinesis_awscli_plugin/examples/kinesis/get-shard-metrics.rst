**get-shard-metrics**

Displays shard-level metrics for the specified stream. By default the datapoints of the last 10 minutes get fetched and averaged. The metric name used by default is 'IncomingRecords'.

** Example 1: **

Simple shard metrics query using the defaults (average of IncomingRecords over the last 15 minutes). Output is always sorted by average in descending order.

aws kinesis get-shard-metrics --stream-name Test

** Example 2: **

Shard metrics query using some another metric and statistic. Output is sorted by average of datapoints for the shards in descending order.

aws kinesis get-shard-metrics --stream-name Test --metric-name WriteProvisionedThroughputExceeded --statistic Sum

** Example 3: **

Use other metric name and different start and end time. Time needs to be in UTC.

aws kinesis get-shard-metrics --stream-name Test --metric-name IncomingBytes --start-time 2016-10-10T10:10:00Z --end-time 2016-10-10T11:10:00Z

if the Python dateparser module is installed (pip install dateparser) you can also use relative dates like --start-time "20 minutes ago"

** Example 4: **

Show only the average of the datapoints over the queried timeframe. Output is also in table format.

aws kinesis get-shard-metrics --stream-name Test --metric-name IncomingBytes --statistic Sum --query "shard_metrics[*].{ShardId:ShardId,Average:Average}" --output table
